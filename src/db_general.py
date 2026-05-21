import datetime
import logging
from venv import logger
import os
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
import psycopg
from psycopg.rows import tuple_row  # default row factory; returns tuples
import public_settings as ps
from candle_buffer import Keys
from indicator_buffer import IndicatorKey as indkeys
logger = logging.getLogger(__name__)
    
load_dotenv()

# ----------------- Client -----------------
def get_pg_conn() -> psycopg.Connection:
    """
    Open a new PostgreSQL connection using environment variables.
    """
    return psycopg.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        dbname=os.getenv("POSTGRES_DB"),
        autocommit=True,
        row_factory=tuple_row,  
    )


# ----------------- Queries -----------------
def get_last_timestamp(
    exchange: str,
    symbol: str,
    timeframe: str,
) -> Optional[Any]:
    """
    Returns the MAX(open_time) from candles or None if empty.
    """
    sql = """
        SELECT MAX(open_time) AS last_candle_open_time
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        row = cur.fetchone()
        # row[0] will be None if no rows
        return row[0] if row else None

def get_candles_from_db(key: Keys, limit: int, until_time: datetime.datetime) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` candles in DESC order, then return ASC list of dicts.
    """
    sql = """
        SELECT
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
          AND close_time <= %s
        ORDER BY open_time DESC
        LIMIT %s
    """

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, until_time, limit))
        rows = cur.fetchall()
    
    # Convert to list of dicts in ASC order
    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in reversed(rows)
    ]
    return candles


def get_candles_from_db_offset(key: Keys, limit: int, offset: int) -> List[Dict[str, Any]]:
    """
    Fetch latest `limit` candles in DESC order with an offset, then return ASC list of dicts.
    """
    sql = """
        SELECT
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE exchange = %s
          AND symbol   = %s
          AND timeframe= %s
        ORDER BY open_time DESC
        OFFSET %s
        LIMIT %s
    """

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (key.exchange, key.symbol, key.timeframe, offset, limit))
        rows = cur.fetchall()
    
    # Convert to list of dicts in ASC order
    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in reversed(rows)
    ]
    return candles



def get_candles_after_time(
    key: Keys,
    after_time: datetime.datetime,
    until_time: Optional[datetime.datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch candles with close_time strictly greater than `after_time`.
    Returns candles in ASC order (oldest -> newest).
    """
    if until_time is None:
        sql = """
            SELECT
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE exchange = %s
              AND symbol   = %s
              AND timeframe= %s
              AND close_time > %s
            ORDER BY close_time ASC
        """
        params = (key.exchange, key.symbol, key.timeframe, after_time)
    else:
        sql = """
            SELECT
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE exchange = %s
              AND symbol   = %s
              AND timeframe= %s
              AND close_time > %s
              AND close_time <= %s
            ORDER BY open_time ASC
        """
        params = (key.exchange, key.symbol, key.timeframe, after_time, until_time)

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in rows
    ]
    return candles


def get_candles_in_range_batch(
    key: Keys,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    limit: int = 1000,
    after_close_time: Optional[datetime.datetime] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch a batch of candles in [start_time, end_time] in ASC close_time order.

    Uses keyset pagination when `after_close_time` is provided so large backtests
    can be replayed incrementally without loading all candles into memory.
    """
    if after_close_time is None:
        sql = """
            SELECT
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE exchange = %s
              AND symbol   = %s
              AND timeframe= %s
              AND close_time >= %s
              AND close_time <= %s
            ORDER BY close_time ASC
            LIMIT %s
        """
        params = (key.exchange, key.symbol, key.timeframe, start_time, end_time, limit)
    else:
        sql = """
            SELECT
                open_time,
                close_time,
                open,
                high,
                low,
                close,
                volume
            FROM candles
            WHERE exchange = %s
              AND symbol   = %s
              AND timeframe= %s
              AND close_time > %s
              AND close_time <= %s
            ORDER BY close_time ASC
            LIMIT %s
        """
        params = (key.exchange, key.symbol, key.timeframe, after_close_time, end_time, limit)

    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in rows
    ]
    return candles


def get_N_last_candles_from_db(
    key: Keys,
    limit: int,
) -> List[Dict[str, Any]]:
    sql = """
        SELECT
            open_time,
            close_time,
            open,
            high,
            low,
            close,
            volume
        FROM candles
        WHERE exchange = %s
            AND symbol   = %s
            AND timeframe= %s
        ORDER BY close_time DESC
        limit %s
    """
    params = (key.exchange, key.symbol, key.timeframe, limit)
 
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    candles = [
        {
            "exchange": key.exchange,
            "symbol": key.symbol,
            "timeframe": key.timeframe,
            "open_time": row[0],
            "close_time": row[1],
            "open": row[2],
            "high": row[3],
            "low": row[4],
            "close": row[5],
            "volume": row[6],
        }
        for row in rows
    ]
    return candles

def get_indicators_from_db(ind_key: indkeys, limit: int) -> List[Dict[str, Any]]:

    sql = """
        SELECT
            event_time,
            value
        FROM strategy_modules_history
        WHERE symbol   = %s
          AND timeframe= %s
          AND key      = %s
          AND ExecuteID = %s
        ORDER BY event_time DESC
        LIMIT %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (ind_key.symbol, ind_key.timeframe, ind_key.key, ps.ExecuteID, limit))
        rows = cur.fetchall()

    indicators = [
        {
            "symbol": ind_key.symbol,
            "timeframe": ind_key.timeframe,
            "key": ind_key.key,
            "event_time": row[0],
            "value": row[1],
        }
        for row in reversed(rows)
    ]
    return indicators

def get_last_bios_from_db(exchange: str, symbol: str, timeframe: str) -> str:
    sql = """
            select bios from bios_signal bs 
            where 
                exchange = %s
                and symbol = %s
                and timeframe = %s
            order by "timestamp" desc
            limit 1
        """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (exchange, symbol, timeframe))
        rows = cur.fetchall()

    for row in reversed(rows):
      return row[0]
    
# ----------------- Public Settings -----------------
def get_public_settings() -> Dict[str, str]:
    sql = "SELECT key, value FROM public.public_settings"
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}

# ----------------- Engine Modules History -----------------
def insert_strategy_modules_history(
    event_time=None,
    symbol=None,
    timeframe=None,
    key=None,
    value=None,
    rows: Optional[List[tuple]] = None,
):
    """
    Insert strategy_modules_history rows.

    Supports two modes:
    1) Single row mode (backward compatible):
       insert_strategy_modules_history(event_time, symbol, timeframe, key, value)
    2) Batch mode:
       insert_strategy_modules_history(rows=[(event_time, symbol, timeframe, key, value), ...])
    """
    sql = """
        INSERT INTO public.strategy_modules_history
        (event_time, symbol, timeframe, key, value, ExecuteID)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        if rows is not None:
            if len(rows) == 0:
                return
            batch = [
                (row[0], row[1], row[2], row[3], row[4], ps.ExecuteID)
                for row in rows
            ]
            cur.executemany(sql, batch)
        else:
            cur.execute(sql, (event_time, symbol, timeframe, key, value, ps.ExecuteID))
    # No return needed

def _insert_signals(rows: List[tuple]) -> None:
    """
    Insert generated signals in one batch.
    """
    sql = """
        INSERT INTO signals (
            event_time,
            signal_symbol,
            confirm_symbols,
            position_type,
            price_source,
            position_price,
            target_pips,
            target_price,
            ref_symbol,
            ref_type,
            pivot_time,
            found_at,
            reject_reason,
            spread,
            sl_pips,
            sl_price,
            correlation_summary,
            order_sent,

            order_sent_time,    
            broker_order_id,    
            broker_trade_id,    
            order_units,        
            actual_entry_time,  
            actual_entry_price, 
            actual_tp_price,    
            actual_target_pips, 
            order_status,       
            exec_latency_ms,
            open_transaction
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    try:
        conn: psycopg.Connection
        conn = get_pg_conn()
        with conn.cursor() as cur:
            cur.executemany(sql, rows)
        conn.commit()
    except Exception as e:
        logger.error(f"[DB ERROR] insert signals failed: {e}")

def get_last_available_indicator_time(ind_key: indkeys) -> Optional[datetime.datetime]:
    sql = """
        SELECT MAX(event_time) AS last_indicator_time
        FROM strategy_modules_history
        WHERE symbol   = %s
          AND timeframe= %s
          AND key      = %s
    """
    with get_pg_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (ind_key.symbol, ind_key.timeframe, ind_key.key))
        row = cur.fetchone()
        return row[0] if row else None

def truncate_backtest_tables() -> None:
    """
    Truncate the following tables to reset state for a fresh backtest run:
    - strategy_modules_history
    - signals
    - pivot_list
    """
    sql_commands = [
        "TRUNCATE TABLE public.strategy_modules_history RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE public.signals RESTART IDENTITY CASCADE;",
        "TRUNCATE TABLE public.pivot_list RESTART IDENTITY CASCADE;",
    ]
    
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            for sql in sql_commands:
                cur.execute(sql)
        logger.info("Successfully truncated strategy_modules_history, signals, and pivot_list tables.")
    except Exception as e:
        logger.error(f"[DB ERROR] Failed to truncate backtest tables: {e}")
        raise