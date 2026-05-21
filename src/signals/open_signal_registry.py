# file: src/signals/open_signal_registry.py
# English-only comments

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from threading import Lock
from typing import Dict, List, Optional, Any, Tuple
import public_moduls as pm
import psycopg

from telegram_notifier import notify_telegram, ChatType

from db_signals import fetch_open_signals_for_open_registry
from db_general import get_pg_conn
import strategy_modules as sm

from orders.order_executor import close_position_by_trade_id

import logging

logger = logging.getLogger(__name__)

def _pip_size(symbol: str) -> Decimal:
    # JPY pairs are typically 0.01 pip size in price terms, and you also special-case DXY
    return Decimal("0.01") if ("JPY" in symbol or "DXY" in symbol) else Decimal("0.0001")


def _to_decimal(x: Any) -> Decimal:
    # Safe conversion for floats/Decimals/strings
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def _pips_distance(price: Decimal, target: Decimal, pip: Decimal) -> Decimal:
    # Absolute distance in pips
    return (price - target).copy_abs() / pip


@dataclass
class OpenSignal:
    exchange: str
    symbol: str
    timeframe: str
    side: str  # "buy" or "sell"
    event_time: datetime
    target_price: Decimal
    position_price: Decimal
    created_at: datetime

    # --- New tracking fields (in-memory) ---
    # Distance metrics are in pips (absolute distance to target).
    nearest_pips_to_target: Optional[Decimal] = None
    farthest_pips_to_target: Optional[Decimal] = None

    # Last tick price used for this signal (BUY->bid, SELL->ask)
    last_tick_price: Optional[Decimal] = None

    # Marks whether metrics changed and should be flushed to DB
    dirty: bool = False

    # Order-related fields (optional at first)
    order_env: Optional[str] = None          # "demo" / "live"
    broker_order_id: Optional[str] = None
    broker_trade_id: Optional[str] = None
    order_units: Optional[int] = None
    actual_entry_time: Optional[datetime] = None
    actual_entry_price: Optional[Decimal] = None
    actual_tp_price: Optional[Decimal] = None
    order_status: str = "none"               # none/pending/open/closed/...
    exec_latency_ms: Optional[int] = None
    trailing_tp_price: Optional[Decimal] = None
    trailing_sl_price: Optional[Decimal] = None
    sl_price: Optional[Decimal] = None

    tp_sl_activated: bool = False  # Whether trailing TP/SL has been activated (for logging/notification purposes)
    sl_activated: bool = False

    actual_exit_time: Optional[datetime] = None
    actual_exit_price: Optional[Decimal] = None
    close_flushed_to_db: bool = False

    target_pips: Optional[Decimal] = None  # For convenience, can be set from target_price/position_price
    sl_pips: Optional[Decimal] = None      # For convenience, can be set from sl_price/position_price


def _estimate_profit_ccy_for_fixed_exit(sig: OpenSignal) -> Optional[Decimal]:
    """
    Estimate close P/L in quote currency for synthetic TP/SL exits.

    Uses target_price, sl_price, and actual_exit_price to determine whether
    the exit behaved like TP or SL, then computes a signed amount based on the
    distance to the opposite bound multiplied by order units.
    """
    if sig.actual_exit_price is None or sig.target_price is None or sig.sl_price is None:
        return None

    exit_price = _to_decimal(sig.actual_exit_price)
    entry_price = _to_decimal(sig.position_price)

    signal_type = sig.side.lower()

    units = Decimal(str(abs(sig.order_units))) if sig.order_units is not None else Decimal("1")

    if signal_type == "buy":
        return (units / Decimal(1000)) * (Decimal(1.5)) * ((exit_price - entry_price) / _pip_size(sig.symbol))
      
    if signal_type == "sell":
        return (units / Decimal(1000)) * (Decimal(1.5)) * ((entry_price - exit_price) / _pip_size(sig.symbol))


class OpenSignalRegistry:
    """
    In-memory registry of open signals that we want to track with ticks.

    This is NOT about order execution logic itself. It only tracks:
      - when a tick reaches target_price for a signal
      - sends a Telegram notification
      - updates the DB row for that signal
      - removes the signal from memory

    Now it also stores optional order-related info for signals whose
    orders were actually sent to the broker, and can be pruned when
    broker closes the trade (sync_broker_orders).
    """

    def __init__(self) -> None:
        self._signals_by_symbol: Dict[str, List[OpenSignal]] = {}
        self._lock = Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def get_count(self) -> int:
        with self._lock:
            return sum(len(v) for v in self._signals_by_symbol.values())

    def get_all_signals(self) -> List[OpenSignal]:
        """Return all signals currently loaded in the in-memory registry."""
        with self._lock:
            return [sig for lst in self._signals_by_symbol.values() for sig in lst]

    def add_signal(self, sig: OpenSignal) -> None:
        """Register a new open signal for tracking."""
        with self._lock:
            lst = self._signals_by_symbol.setdefault(sig.symbol, [])
            lst.append(sig)

    def add_signal_to_open_registry(
        self,
        rows: List[tuple],
        exchange: str,
        timeframe: str,
    ) -> int:
        """Add raw DB rows (from _insert_signals) into the in-memory open registry.

        The `rows` list uses the same column ordering as `_insert_signals()` in
        `db_general.py`. We map the relevant fields into an OpenSignal instance.
        """

        added = 0
        with self._lock:
            for row in rows:
                # Row layout (see db_general._insert_signals):
                # 0 event_time, 1 signal_symbol, 2 confirm_symbols, 3 position_type,
                # 4 price_source, 5 position_price, 6 target_pips, 7 target_price,
                # 8 ref_symbol, 9 ref_type, 10 pivot_time, 11 found_at,
                # 12 reject_reason, 13 spread, 14 sl_pips, 15 sl_price,
                # 16 correlation_summary, 17 order_sent, 18 order_sent_time,
                # 19 broker_order_id, 20 broker_trade_id, 21 order_units,
                # 22 actual_entry_time, 23 actual_entry_price, 24 actual_tp_price,
                # 25 actual_target_pips, 26 order_status, 27 exec_latency_ms,
                # 28 open_transaction
                sig = OpenSignal(
                    exchange=exchange,
                    symbol=str(row[1]),
                    timeframe=timeframe,
                    side=str(row[3]).lower(),
                    event_time=row[0],
                    target_price=_to_decimal(row[7]),
                    position_price=_to_decimal(row[5]),
                    created_at=row[0],
                    order_env=None,
                    broker_order_id=row[19],
                    broker_trade_id=row[20],
                    order_units=row[21],
                    actual_entry_time=row[22],
                    actual_entry_price=_to_decimal(row[23]) if row[23] is not None else None,
                    actual_tp_price=_to_decimal(row[24]) if row[24] is not None else None,
                    order_status=str(row[26] or "none"),
                    exec_latency_ms=row[27],
                )
                self._signals_by_symbol.setdefault(sig.symbol, []).append(sig)
                added += 1

        return added

    def bootstrap_from_db(self, conn: psycopg.Connection, symbol: str) -> int:
        """
        Load ALL open signals from DB into memory.

        NOTE:
        - This does not modify SignalRegistry and does not affect dedup logic.
        - This is only to populate OpenSignalRegistry so it can track ticks
          and update nearest/farthest pips.

        The DB SELECT is delegated to db_signals.py (as requested).
        """
        if fetch_open_signals_for_open_registry is None:
            raise RuntimeError(
                "fetch_open_signals_for_open_registry is not available. "
                "Implement and export it from database/db_signals.py"
            )

        rows = fetch_open_signals_for_open_registry(conn, symbol)  # type: ignore
        loaded = 0

        with self._lock:
            # Clear existing in-memory signals before reloading from the DB.
            # This ensures we don't keep stale signals from previous loads.
            self._signals_by_symbol.clear()

            for row in rows:
                sig = OpenSignal(
                    exchange=str(row["exchange"]),
                    symbol=str(row["symbol"]),
                    timeframe=str(row["timeframe"]),
                    side=str(row["side"]).lower(),
                    event_time=row["event_time"],
                    target_price=_to_decimal(row["target_price"]),
                    position_price=_to_decimal(row["position_price"]),
                    created_at=row["created_at"],
                    # If your DB loader returns these, we accept them; otherwise remain None
                    nearest_pips_to_target=_to_decimal(row["nearest_pips_to_target"]) if row.get("nearest_pips_to_target") is not None else None,  # type: ignore
                    farthest_pips_to_target=_to_decimal(row["farthest_pips_to_target"]) if row.get("farthest_pips_to_target") is not None else None,  # type: ignore
                    last_tick_price=_to_decimal(row["last_tick_price"]) if row.get("last_tick_price") is not None else None,  # type: ignore
                    order_env=row.get("order_env"),
                    broker_order_id=row.get("broker_order_id"),
                    broker_trade_id=row.get("broker_trade_id"),
                    order_units=row.get("order_units"),
                    actual_entry_time=row.get("actual_entry_time"),
                    actual_entry_price=_to_decimal(row["actual_entry_price"]) if row.get("actual_entry_price") is not None else None,  # type: ignore
                    actual_tp_price=_to_decimal(row["actual_tp_price"]) if row.get("actual_tp_price") is not None else None,  # type: ignore
                    order_status=str(row.get("order_status") or "none"),
                    exec_latency_ms=row.get("exec_latency_ms"),
                    trailing_tp_price=_to_decimal(row["trailing_tp_price"]) if row.get("trailing_tp_price") is not None else None,  # type: ignore
                    sl_price=_to_decimal(row["sl_price"]) if row.get("sl_price") is not None else None,  # type: ignore
                    trailing_sl_price=_to_decimal(row["trailing_sl_price"]) if row.get("trailing_sl_price") is not None else None,  # type: ignore
                    target_pips=_to_decimal(row["target_pips"]) if row.get("target_pips") is not None else None,  # type: ignore
                    sl_pips=_to_decimal(row["sl_pips"]) if row.get("sl_pips") is not None else None,  # type: ignore
                )

                self._signals_by_symbol.setdefault(sig.symbol, []).append(sig)
                loaded += 1

        return loaded
    
    def attach_order_info(
        self,
        *,
        symbol: str,
        side: str,
        event_time: datetime,
        order_env: str,
        broker_order_id: Optional[str],
        broker_trade_id: Optional[str],
        order_units: Optional[int],
        actual_entry_time: Optional[datetime],
        actual_entry_price: Optional[Decimal],
        actual_tp_price: Optional[Decimal],
        order_status: str,
        exec_latency_ms: Optional[int],
    ) -> None:
        """
        Find the matching OpenSignal (by symbol, side, event_time) and
        attach order-related information to it.
        """
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            for sig in signals:
                if (
                    sig.side.lower() == side.lower()
                    and sig.event_time == event_time
                ):
                    sig.order_env = order_env
                    sig.broker_order_id = broker_order_id
                    sig.broker_trade_id = broker_trade_id
                    sig.order_units = order_units
                    sig.actual_entry_time = actual_entry_time
                    sig.actual_entry_price = actual_entry_price
                    sig.actual_tp_price = actual_tp_price
                    sig.order_status = order_status
                    sig.exec_latency_ms = exec_latency_ms
                    break

    def remove_by_broker(
        self,
        *,
        symbol: str,
        side: str,
        event_time: datetime,
    ) -> None:
        """
        Remove a signal when broker confirms the trade is closed.

        Called from sync_broker_orders() using the same key triple
        (symbol, side, event_time) that we use for attach_order_info.
        """
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            survivors: List[OpenSignal] = []
            for sig in signals:
                if (
                    sig.side.lower() == side.lower()
                    and sig.event_time == event_time
                ):
                    # Drop this one
                    continue
                survivors.append(sig)

            if survivors:
                self._signals_by_symbol[symbol] = survivors
            else:
                self._signals_by_symbol.pop(symbol, None)
            
            logger.warning(f"Dropped from open_signal_registry: symbol:{symbol}, side: {side}, event_time: {event_time}")

    def _manage_trailing_sl(self, *, sig: OpenSignal, current_price: Decimal) -> None:
        
        try:

            if sig.side.lower() == 'buy':
                if sig.trailing_tp_price is not None:
                    sig.trailing_tp_price = Decimal(sig.trailing_tp_price) + Decimal(0.0002)
                else:
                    sig.trailing_tp_price = Decimal(sig.actual_tp_price) + Decimal(0.0002)

                if sig.trailing_sl_price is not None:
                    sig.trailing_sl_price = Decimal(sig.trailing_sl_price) + Decimal(0.0002)
                else:
                    sig.trailing_sl_price = Decimal(sig.sl_price) + Decimal(0.0002)
            else:
                if sig.trailing_tp_price is not None:
                    sig.trailing_tp_price = Decimal(sig.trailing_tp_price) - Decimal(0.0002)
                else:
                    sig.trailing_tp_price = Decimal(sig.actual_tp_price) - Decimal(0.0002)

                if sig.trailing_sl_price is not None:
                    sig.trailing_sl_price = Decimal(sig.trailing_sl_price) - Decimal(0.0002)
                else:
                    sig.trailing_sl_price = Decimal(sig.sl_price) - Decimal(0.0002)

            sig.dirty = True

        except Exception as e:
            print(f"[WARN] manage_trailing_sl failed: {e}")    

        return


    def process_TSL_candle_close(self, candle_body: Dict[str, Any]) -> None:
 
        symbol = candle_body["symbol"]
        exchange = candle_body["exchange"]
        close = Decimal(str(round(candle_body["close"], 5)))
        open = Decimal(str(round(candle_body["open"], 5)))
        low = Decimal(str(round(candle_body["low"], 5)))
        high = Decimal(str(round(candle_body["high"], 5)))
        new_TSL_price = None

        '''
        green_candle = False
        red_candle = False

        if close == open:
            return
        
        if close > open:
            green_candle = True
        else:
            red_candle = True
        '''


        '''
        # This is the price level we use to check whether we can move up the trailing SL; it's typically the mid or n percent of the current same direction candle
        if green_candle:
            follow_price = open + ((close - open) * ps.New_Candle_Follow_Price_pct)
        if red_candle:
            follow_price = open - ((open - close) * ps.New_Candle_Follow_Price_pct)
        '''

        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            for sig in signals:
                if sig.exchange != exchange:
                    continue
                
                if sig.side.lower() == "buy" and sig.actual_tp_price <= high:
                    new_TSL_price = high - (sm.ATR * Decimal(1))
                    if (sig.trailing_sl_price is None) or (new_TSL_price > sig.trailing_sl_price):
                        sig.trailing_sl_price = new_TSL_price
                        sig.dirty = True

                        logger.info(f"Price followed for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sig.trailing_sl_price}")
                        '''
                        msg = (
                                f"🍬 Trailing SL Activated\n\n"
                                f"Symbol:         {sig.symbol}\n"
                                f"Side:              {sig.side}\n"
                                f"tsl_price:               {sig.trailing_sl_price.quantize(Decimal('0.00001'))}\n"
                                f"Est. profit pips:       {pm.truncate((sig.trailing_sl_price - sig.actual_entry_price) / Decimal('0.0001'))}\n\n"
                                f"Event time:  {sig.event_time}\n"
                            )
                        notify_telegram(msg, ChatType.INFO)
                        '''


                if sig.side.lower() == "sell" and sig.actual_tp_price >= low:
                    new_TSL_price = low + (sm.ATR * Decimal(1))
                    if (sig.trailing_sl_price is None) or (new_TSL_price < sig.trailing_sl_price):
                        sig.trailing_sl_price = new_TSL_price

                        sig.dirty = True
                        logger.info(f"Price followed for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sig.trailing_sl_price}")
                        '''
                        msg = (
                                f"🍬 Trailing SL Activated\n\n"
                                f"Symbol:         {sig.symbol}\n"
                                f"Side:              {sig.side}\n"
                                f"tsl_price:               {sig.trailing_sl_price.quantize(Decimal('0.00001'))}\n"
                                f"Est. profit pips:       {pm.truncate((sig.actual_entry_price - sig.trailing_sl_price) / Decimal('0.0001'))}\n\n"
                                f"Event time:  {sig.event_time}\n"
                            )
                        notify_telegram(msg, ChatType.INFO)
                        '''
                #=====================================================================================================================
                if sig.side.lower() == "buy" and sig.dirty == False:
                    p1RR = (sig.actual_tp_price - sig.actual_entry_price) / Decimal(2)
                    p1RR = sig.actual_entry_price + p1RR
                    if p1RR <= high and p1RR >= low:
                        sig.trailing_sl_price = sig.actual_entry_price + ((sig.actual_tp_price - sig.actual_entry_price) / Decimal(10))
                        sig.dirty = True

                if sig.side.lower() == "sell" and sig.dirty == False:
                    p1RR = (sig.actual_entry_price - sig.actual_tp_price) / Decimal(2)
                    p1RR = sig.actual_entry_price - p1RR
                    if p1RR <= high and p1RR >= low:
                        sig.trailing_sl_price = sig.actual_entry_price - ((sig.actual_entry_price - sig.actual_tp_price) / Decimal(10))
                        sig.dirty = True

    def _apply_fix_tp_sl(self, *, sig: OpenSignal, current_price: Decimal, close_time: datetime, CE_LONG: Optional[Decimal] = None, CE_SHORT: Optional[Decimal] = None) -> None:
        
        if sig.tp_sl_activated:
            return

        try:
            
            if sig.side.lower() == "buy" and current_price == CE_LONG:
                logger.info(f"Applying Fix TP for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, tp_price:{sig.actual_tp_price}, current_price:{current_price}")
                sig.tp_sl_activated = True
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time
            

            if sig.side.lower() == "buy" and current_price <= sl_price_to_check:
                logger.info(f"Applying Fix SL for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sl_price_to_check}, current_price:{current_price}")
                sig.tp_sl_activated = True        
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time
                
            
            if sig.side.lower() == "sell" and current_price == CE_SHORT:
                logger.info(f"Applying Fix TP for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, tp_price:{sig.actual_tp_price}, current_price:{current_price}")
                sig.tp_sl_activated = True        
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time
            
            
            if sig.side.lower() == "sell" and current_price >= sl_price_to_check:
                logger.info(f"Applying Fix SL for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sl_price_to_check}, current_price:{current_price}")
                sig.tp_sl_activated = True        
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time

        except Exception as e:
            logger.error(f"Error in _apply_fix_sl: {e}")

        return
      
    def _apply_fix_tp_sl_2(self, *, sig: OpenSignal, current_price: Decimal, close_time: datetime) -> None:
        
        if sig.tp_sl_activated:
            return

        try:
            
            if sig.side.lower() == "buy" and current_price <= sig.sl_price:
                logger.info(f"Applying Fix SL for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sig.sl_price}, current_price:{current_price}")
                sig.tp_sl_activated = True        
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time
                
            if sig.side.lower() == "sell" and current_price >= sig.sl_price:
                logger.info(f"Applying Fix SL for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sig.sl_price}, current_price:{current_price}")
                sig.tp_sl_activated = True        
                sig.sl_activated = True
                sig.order_status = "closed"
                sig.actual_exit_price = current_price
                sig.actual_exit_time = close_time

        except Exception as e:
            logger.error(f"Error in _apply_fix_sl: {e}")

        return

    def process_tick_for_symbol(
        self,
        *,
        exchange: str,
        symbol: str,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        now: datetime,
        conn: Optional[psycopg.Connection] = None,
        CE_LONG: Optional[Decimal] = None,
        CE_SHORT: Optional[Decimal] = None,
    ) -> None:
                
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            for sig in signals:
                # Basic sanity check: same exchange
                if sig.exchange != exchange:
                    continue

                if sig.target_price <= high and sig.target_price >= low:
                    #tp is hit, we check with tp price
                    logger.info(f"Applying Fix TP for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, tp_price:{sig.actual_tp_price}, current_price:{sig.actual_tp_price}")
                    sig.tp_sl_activated = True
                    sig.sl_activated = True
                    sig.order_status = "closed"
                    sig.actual_exit_price = sig.target_price
                    sig.actual_exit_time = now  

                if sig.sl_price <= high and sig.sl_price >= low:
                    #sl is hit, we check with sl price
                    logger.info(f"Applying Fix SL for {sig.side} signal: symbol:{sig.symbol}, trade_id:{sig.broker_trade_id}, sl_price:{sig.sl_price}, current_price:{sig.sl_price}")
                    sig.tp_sl_activated = True        
                    sig.sl_activated = True
                    sig.order_status = "closed"
                    sig.actual_exit_price = sig.sl_price
                    sig.actual_exit_time = now

        self.flush_activated_signals(conn=conn, symbol=symbol)

    def flush_activated_signals(
        self,
        conn: Optional[psycopg.Connection] = None,
        symbol: Optional[str] = None,
    ) -> int:
        """
        Persist TP/SL-activated closures from in-memory open registry to DB.

        For every signal where tp_sl_activated=True, update:
          - order_status
          - actual_exit_price
          - actual_exit_time
          - profit_ccy (estimated from target/sl/exit)
        """
        pending: List[OpenSignal] = []

        with self._lock:
            if symbol is None:
                all_lists = self._signals_by_symbol.values()
            else:
                all_lists = [self._signals_by_symbol.get(symbol, [])]

            for signals in all_lists:
                for sig in signals:
                    if not (sig.tp_sl_activated or sig.sl_activated):
                        continue
                    if sig.close_flushed_to_db:
                        continue
                    if sig.actual_exit_time is None or sig.actual_exit_price is None:
                        continue
                    pending.append(sig)

        if not pending:
            return 0

        own_conn = False
        if conn is None:
            conn = get_pg_conn()
            own_conn = True

        updated = 0
        sql = """
            UPDATE public.signals
               SET order_status = %s,
                   actual_exit_price = %s,
                   actual_exit_time = %s,
                   profit_ccy = %s
             WHERE signal_symbol = %s
               AND position_type = %s
               AND event_time = %s
               AND order_sent = true
               AND actual_exit_time IS NULL
        """

        try:
            with conn.cursor() as cur:
                for sig in pending:
                    profit_ccy = _estimate_profit_ccy_for_fixed_exit(sig)
                    cur.execute(
                        sql,
                        (
                            sig.order_status,
                            sig.actual_exit_price,
                            sig.actual_exit_time,
                            profit_ccy,
                            sig.symbol,
                            sig.side,
                            sig.event_time,
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        sig.close_flushed_to_db = True
                        updated += 1

            conn.commit()
        except Exception as e:
            logger.error(f"[open_signals] flush_activated_signals failed: {e}")
            if conn is not None:
                conn.rollback()
        finally:
            if own_conn and conn is not None:
                conn.close()

        if updated > 0:
            logger.info(f"[open_signals] flush_activated_signals: updated={updated}")

        return updated

      
    def process_tick_for_symbol_2(
        self,
        *,
        exchange: str,
        symbol: str,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        now: datetime,
        candle_body: Dict[str, Any],
        conn: Optional[psycopg.Connection] = None,
    ) -> None:
        """Evaluate TP/SL hits for one symbol and flush activated closes to DB."""
        if high > low:
            candle_type = "green"
        else:
            candle_type = "red"
                
        with self._lock:
            signals = self._signals_by_symbol.get(symbol)
            if not signals:
                return

            for sig in signals:
                # Basic sanity check: same exchange
                if sig.exchange != exchange:
                    continue
                
                if sig.sl_price <= high and sig.sl_price >= low:
                    #sl is hit, we check with sl price
                    price_to_check = sig.sl_price
                else:
                    # in this case, we check with close price which still is not causing the hit but we want the flow works
                    price_to_check = close

                # Manage trailing SL if applicable
                #self._manage_trailing_sl(sig=sig, current_price=price_to_check)

                # Apply trailing SL if applicable
                #self._apply_trailing_sl(sig=sig, current_price=price_to_check)

                # Apply fix TP/SL if applicable
                self._apply_fix_tp_sl_2(sig=sig, current_price=price_to_check, close_time = now)
                self.process_TSL_candle_close(candle_body=candle_body)

        self.flush_activated_signals_2(conn=conn, symbol=symbol)

    def flush_activated_signals_2(
        self,
        conn: Optional[psycopg.Connection] = None,
        symbol: Optional[str] = None,
    ) -> int:
        """
        Persist TP/SL-activated closures from in-memory open registry to DB.

        For every signal where tp_sl_activated=True, update:
          - order_status
          - actual_exit_price
          - actual_exit_time
          - profit_ccy (estimated from target/sl/exit)
        """
        pending: List[OpenSignal] = []

        with self._lock:
            if symbol is None:
                all_lists = self._signals_by_symbol.values()
            else:
                all_lists = [self._signals_by_symbol.get(symbol, [])]

            for signals in all_lists:
                for sig in signals:
                    if not (sig.tp_sl_activated or sig.sl_activated or sig.dirty):
                        continue
                    if sig.close_flushed_to_db:
                        continue
                    pending.append(sig)

        if not pending:
            return 0

        own_conn = False
        if conn is None:
            conn = get_pg_conn()
            own_conn = True

        updated = 0
        sql = """
            UPDATE public.signals
               SET order_status = %s,
                   actual_exit_price = %s,
                   actual_exit_time = %s,
                   profit_ccy = %s,
                   sl_price = %s
             WHERE signal_symbol = %s
               AND position_type = %s
               AND event_time = %s
               AND order_sent = true
               AND actual_exit_time IS NULL
        """

        try:
            with conn.cursor() as cur:
                for sig in pending:
                    profit_ccy = _estimate_profit_ccy_for_fixed_exit(sig)
                    cur.execute(
                        sql,
                        (
                            sig.order_status,
                            sig.actual_exit_price,
                            sig.actual_exit_time,
                            profit_ccy,
                            sig.sl_price,
                            sig.symbol,
                            sig.side,
                            sig.event_time,
                        ),
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        sig.close_flushed_to_db = True
                        updated += 1

            conn.commit()
        except Exception as e:
            logger.error(f"[open_signals] flush_activated_signals failed: {e}")
            if conn is not None:
                conn.rollback()
        finally:
            if own_conn and conn is not None:
                conn.close()

        if updated > 0:
            logger.info(f"[open_signals] flush_activated_signals: updated={updated}")

        return updated

    def flush_distance_metrics(self, conn: psycopg.Connection) -> int:
        """
        Persist nearest/farthest pips metrics + last_tick_price to DB for signals that changed.

        We intentionally DO NOT use the identity id column.
        We update using:
          symbol + side + event_time + target_price
        plus open constraints:
          order_sent=true AND actual_exit_time IS NULL

        Also updates updatetime = NOW() in PostgreSQL.
        """
        to_flush: List[Tuple[OpenSignal, Decimal, Decimal]] = []
        
        with self._lock:
            for _, signals in self._signals_by_symbol.items():
                for sig in signals:
                    if not sig.dirty:
                        continue
                    if sig.trailing_tp_price is None and sig.trailing_sl_price is None:
                        continue
                    to_flush.append((sig, sig.trailing_tp_price, sig.trailing_sl_price))
                    # Mark clean now; if DB fails, we'll mark dirty again on next ticks.
                    sig.dirty = False

        if not to_flush:
            #logger.info(f"[open_signals] flush_trailing_prices: No updated trailing_prices for open signals")            
            return 0

        sql = """
            UPDATE public.signals
               SET trailing_tp_price = %s,
                   trailing_sl_price = %s,
                   tick_update_time = NOW()
             WHERE signal_symbol = %s
               AND position_type = %s
               AND event_time    = %s
               AND order_sent = true
               AND actual_exit_time IS NULL
        """

        updated = 0
        try:
            with conn.cursor() as cur:
                for sig, trailing_tp_price, trailing_sl_price in to_flush:
                    cur.execute(
                        sql,
                        (
                            trailing_tp_price,
                            trailing_sl_price,
                            sig.symbol,
                            sig.side,
                            sig.event_time,
                        ),
                    )
                    updated += 1
            conn.commit()

            logger.info(f"[open_signals] flush_trailing_prices: for {updated} open signals")
                    
        except Exception as e:
            print(f"[WARN] flush_trailing_prices failed: {e}")

        return updated


    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------


    def _update_distance_metrics(self, *, sig: OpenSignal, price_to_check: Decimal) -> None:
        """
        Update nearest/farthest absolute distance to target in pips.
        Also marks signal dirty if values change.
        """
        pip = _pip_size(sig.symbol)
        dist_pips = _pips_distance(price=price_to_check, target=sig.target_price, pip=pip)

        # First tick seen for this signal
        if sig.nearest_pips_to_target is None or sig.farthest_pips_to_target is None:
            sig.nearest_pips_to_target = dist_pips
            sig.farthest_pips_to_target = dist_pips
            sig.dirty = True
            return

        changed = False
        if dist_pips < sig.nearest_pips_to_target:
            sig.nearest_pips_to_target = dist_pips
            changed = True
        if dist_pips > sig.farthest_pips_to_target:
            sig.farthest_pips_to_target = dist_pips
            changed = True

        # last_tick_price changed every tick; we only flush when metrics changed OR you can force flush always.
        # If you want last_tick_price always flushed, set dirty=True unconditionally on each tick.
        if changed:
            sig.dirty = True


    def _on_signal_hit(
        self,
        *,
        sig: OpenSignal,
        hit_price: Decimal,
        hit_time: datetime,
        conn: Optional[psycopg.Connection],
    ) -> None:
        """Handle a signal that has reached its target."""

        # 1) Telegram notification
        try:
            # Calculate actual realized pips at hit
            pip_size = Decimal("0.01") if ("JPY" in sig.symbol or "DXY" in sig.symbol) else Decimal("0.0001")
            pips_realized = (hit_price - sig.position_price) / pip_size

            # For BUY, positive pips are profit; for SELL reverse sign
            if sig.side.lower() == "sell":
                pips_realized = -pips_realized

            # Dollar profit with assumed $5000 position size
            profit_usd = pips_realized / Decimal("10000") * Decimal("5000")

            '''
            msg = (
                "🎯 TARGET HIT\n"
                f"Symbol:         {sig.symbol}\n"
                f"Side:           {sig.side.upper()}\n\n"
                f"Entry price:    {sig.position_price}\n"
                f"Target price:   {sig.target_price}\n"
                f"Hit price:      {hit_price}\n\n"
                f"Pips gained:    {pips_realized:.1f}\n"
                f"Profit:         ${profit_usd:.2f}\n\n"
                f"Event time:     {sig.event_time.strftime('%Y-%m-%d %H:%M')}\n"
                f"Hit time:       {hit_time.strftime('%Y-%m-%d %H:%M')}\n"
            )
            
            notify_telegram(msg, ChatType.INFO)
            
            '''
            
        except Exception as e:
            print(f"[WARN] telegram notify (target hit) failed: {e}")

        # 2) DB update (if connection is provided)
        if conn is None:
            return

        try:
            sql = """
                UPDATE signals
                   SET hit_price = %s,
                       hit_time  = %s
                 WHERE signal_symbol = %s
                   AND position_type = %s
                   AND event_time    = %s
                   AND target_price  = %s
            """
            with conn.cursor() as cur:
                cur.execute(
                    sql,
                    (
                        hit_price,
                        hit_time,
                        sig.symbol,
                        sig.side,
                        sig.event_time,
                        sig.target_price,
                    ),
                )
            conn.commit()
        except Exception as e:
            print(f"[WARN] failed to update signals(hit_price, hit_time): {e}")


# ----------------------------------------------------------------------
# Global provider
# ----------------------------------------------------------------------

_GLOBAL_OPEN_SIGNAL_REGISTRY: Optional[OpenSignalRegistry] = None


def get_open_signal_registry() -> OpenSignalRegistry:
    global _GLOBAL_OPEN_SIGNAL_REGISTRY
    if _GLOBAL_OPEN_SIGNAL_REGISTRY is None:
        _GLOBAL_OPEN_SIGNAL_REGISTRY = OpenSignalRegistry()
    return _GLOBAL_OPEN_SIGNAL_REGISTRY
