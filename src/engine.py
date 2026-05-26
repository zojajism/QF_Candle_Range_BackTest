from decimal import Decimal
import json
import logging
from os import truncate
from typing import Any, Dict, List
from urllib import response
from venv import logger
from datetime import datetime
import buffer_initializer

import requests

from db_general import _insert_signals
from orders.order_executor import sync_broker_orders
from candle_buffer import Keys
from indicator_buffer import IndicatorKey 
import strategy_modules as sm
import buffer_initializer as buffers
import public_moduls as pm 
import public_settings as ps 
from telegram_notifier import (
    notify_telegram,
    ChatType,
    start_telegram_notifier,
    close_telegram_notifier,
    ChatType,
)
from orders.order_executor import send_market_order, OrderExecutionResult, update_account_summary
from signals import open_signal_registry
from db_general import get_pg_conn


logger = logging.getLogger(__name__)


def run_engine(candle_body: Dict[str, Any]):
    '''
    "exchange": exchange,
    "symbol": payload_symbol,
    "base_currency": base_currency.upper(),
    "quote_currency": quote_currency.upper(),
    "timeframe": timeframe,
    "open_time": open_time_iso,
    "close_time": close_time_iso,
    "open": float(data["open"]),
    "high": float(data["high"]),
    "low": float(data["low"]),
    "close": float(data["close"]),
    "volume": float(data["volume"]),
    # Publisher insert timestamp (UTC ISO)
    "insert_ts": _ensure_utc_iso(datetime.now(timezone.utc)),
    '''
    if isinstance(candle_body.get("open_time"), str):
        candle_body["open_time"] = datetime.fromisoformat(candle_body["open_time"])
    if isinstance(candle_body.get("close_time"), str):
        candle_body["close_time"] = datetime.fromisoformat(candle_body["close_time"])
    
    # Check if the received candle is newer than the last candle in the buffer to avoid processing duplicates
    key = Keys(exchange=candle_body["exchange"], symbol=candle_body["symbol"], timeframe=candle_body["timeframe"])
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)
    if candles and candles[0]["close_time"] == candle_body["close_time"]:
        logger.warning(f"SKIP PROCESSING THE CANDLE. Received candle with close_time {candle_body['close_time']}")
        return 
    #==============================================================================================================

    symbol = candle_body["symbol"]
    timeframe = candle_body["timeframe"] 
    exchange = candle_body["exchange"]   
    close_time = candle_body["close_time"]
    close = Decimal(str(round(candle_body["close"], 5)))

    sm.symbol = symbol
    sm.timeframe = timeframe
    sm.exchange = exchange
    sm.close_time = close_time

    logger.info(f"Candle received: {sm.symbol}, {sm.timeframe}, {pm.format_time_simple(str(sm.close_time))}, {close}, Candle_counter: {sm.Candle_Count_After_HTF_Reset}")

    # Append candle to the list =============================================================================================
    key = Keys(sm.exchange, sm.symbol, sm.timeframe)
    buffers.CANDLE_BUFFER.append(key, candle_body)
    #========================================================================================================================

    sm.Candle_Count_After_HTF_Reset += 1
    
    #Manage HTF
    sm.manage_HTF()

    #=========================================================================================================================

    # Call engine modules to calculate engine parameters ====================================================================
    sm.calculate_ATR(sm.timeframe)
    sm.calculate_MACD()
    sm.calculate_RSI()
    sm.calculate_EMA(speed="fast")
    sm.calculate_EMA(speed="slow")
    '''
    sm.calculate_ADX(sm.timeframe)
    sm.calc_norm_slope()
    sm.check_trend()
    sm.check_close_in_Keltner_Bands()
    sm.is_bearish_engulfing_candle()
    sm.is_bullish_engulfing_candle()
    sm.is_hammer_candle()
    sm.is_shooting_star_candle()
    sm.is_3candle_strike_bullish()
    sm.is_3candle_strike_bearish()
    sm.calculate_Chandelier_Exit()
    sm.calculate_Volume_AVG()
    sm.update_pivot_buffer(sm.timeframe)
    '''
    #========================================================================================================================



    # Decision Engine
    order_is_allowed = True
    if pm.validate_trading_hours(sm.close_time) == False:
        order_is_allowed = False

    if sm.Candle_Count_After_HTF_Reset != 1:
        order_is_allowed = False

    # if all general conditions are met, then check strategy specific conditions and send order
    if order_is_allowed:
        
        try:
            valid_signal, side, TP, SL = sm.is_valid_signal_HTF_Range()
        except Exception as e:
            logger.error(f"Error in strategy signal detection: {e}")
            valid_signal = False

        # We want to trade only two times per day
        #if valid_signal:
            #valid_signal = sm.Valid_Signal_Counter(sm.close_time)

        if valid_signal:
            target_pips = abs(close - Decimal(TP)) * 10000
            sl_pips = abs(close - Decimal(SL)) * 10000
            
            order_units = ps.DEFAULT_ORDER_UNITS
            #profit_est = (order_units * target_pips / Decimal("10000"))
            profit_est = target_pips * Decimal(1.5) * (order_units / Decimal("1000"))
            logger.info(f"Valid signal detected! side={side}, target_pips={target_pips:.1f}, sl_pips={sl_pips:.1f}, profit_est={profit_est:.2f}, TP={TP}, SL={SL}")

            # Handle Signals in DB

            actual_target_pips = target_pips
            batch_rows_signals: List[tuple] = []
            batch_rows_signals.append((
                close_time,             # event_time (trigger)
                symbol,                 # signal_symbol
                "",                     # confirm_symbols
                side.lower(),           # position_type
                "",                     # price_source
                close,                  # position_price
                target_pips,            # target_pips (ADJUSTED, magnitude)
                TP,                     # target_price (ADJUSTED)
                "",                     # ref_symbol (context)
                "",                     # ref_type (context)
                close_time,             # pivot_time (ref anchor)
                close_time,             # found_at (target pivot time)
                "",                     # reject_reason
                0.0,                    # spread
                sl_pips,                # sl_pips
                SL,                     # sl_price
                "",                     # correlation_summary
                True,                    # order_sent


                close_time,   # order_sent_time
                -1,   # broker_order_id
                -1,   # broker_trade_id
                order_units,             # order_units
                close_time, # actual_entry_time
                close, # actual_entry_price
                TP,   # actual_tp_price
                actual_target_pips,           # actual_target_pips
                "open",            # order_status
                0,   # exec_latency_ms
                -1,          # lastTransactionID from OANDA response
            ))
            _insert_signals(batch_rows_signals)

    # End of : "if all general conditions are met, then check strategy specific conditions and send order"
    else:
        i = 0
        #logger.info("Order not allowed due to trading hours or other general conditions.")  
    #=====================================================================================================================



    #open_count = sm.open_sig_registry.get_count() 
    #logger.info(json.dumps({ "EventCode": 0, "Message": f"open_sig_registry initialized. open_signals={open_count}" }) )
    

    sm.open_sig_registry.process_tick_for_symbol(
                            exchange="OANDA",
                            symbol=symbol,
                            high=Decimal(str(candle_body["high"])),
                            low=Decimal(str(candle_body["low"])),
                            close=Decimal(str(candle_body["close"])),
                            now=sm.close_time,
                            conn=None,
                            CE_LONG=sm.CE_LONG,
                            CE_SHORT=sm.CE_SHORT,   
                        )
    
    #flush open signal registry to db
    #sm.open_sig_registry.flush_distance_metrics(get_pg_conn())
    
    #update open signal registry from db
    sm.open_sig_registry.bootstrap_from_db(get_pg_conn(), ps.symbol) 
        
    # Records values and history
    sm.record_strategy_modules_history(sm.timeframe)
    #========================================================================================================================
