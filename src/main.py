import asyncio
import json
from pathlib import Path
import datetime
import yaml
from dotenv import load_dotenv
from logger_config import setup_logger
import public_settings
import public_settings as ps
from indicator_gap_fill import fill_indicator_gap
import strategy_modules as sm
from engine import run_engine

from db_general import get_pg_conn, get_candles_in_range_batch, truncate_backtest_tables
from candle_buffer import Keys

from telegram_notifier import (
    notify_telegram,
    ChatType,
    start_telegram_notifier,
    close_telegram_notifier,
)

import buffer_initializer as buffers
from public_settings import load_settings_from_db

CONFIG_PATH = Path("/data/config.yaml")
if not CONFIG_PATH.exists():
    CONFIG_PATH = Path(__file__).resolve().parent / "data" / "config.yaml"

def _parse_config_dt(value: str) -> datetime.datetime:
    value = str(value).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed


def _to_engine_payload(candle: dict) -> dict:
    symbol = str(candle["symbol"])
    base_currency = ""
    quote_currency = ""
    if "/" in symbol:
        base_currency, quote_currency = symbol.split("/", 1)

    open_time = candle["open_time"]
    close_time = candle["close_time"]
    open_time_iso = open_time.isoformat() if hasattr(open_time, "isoformat") else str(open_time)
    close_time_iso = close_time.isoformat() if hasattr(close_time, "isoformat") else str(close_time)

    return {
        "exchange": candle["exchange"],
        "symbol": symbol,
        "base_currency": base_currency,
        "quote_currency": quote_currency,
        "timeframe": candle["timeframe"],
        "open_time": open_time_iso,
        "close_time": close_time_iso,
        "open": float(candle["open"]),
        "high": float(candle["high"]),
        "low": float(candle["low"]),
        "close": float(candle["close"]),
        "volume": float(candle["volume"]),
    }


async def main():
    try:


        # Load .env (Docker volume first, then local)
        env_path = Path("/data/.env")
        if not env_path.exists():
            env_path = Path(__file__).resolve().parent / "data" / ".env"
        load_dotenv(dotenv_path=env_path)


        DB_Conn = get_pg_conn()

        truncate_backtest_tables()

        logger = setup_logger()
        logger.info(
            json.dumps(
                {
                    "EventCode": 0,
                    "Message": "Starting QF_RAEP_BT …",
                }
            )
        )
        await start_telegram_notifier()
        notify_telegram("❇️ QF_RAEP_BT App started …", ChatType.ALERT)

        # Load config
        if not CONFIG_PATH.exists():
            raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")

        with CONFIG_PATH.open("r", encoding="utf-8") as f:
            config_data = yaml.safe_load(f) or {}

        # Read symbols/timeframes from config for DB backtest scope.
        symbols_cfg = [str(s) for s in config_data.get("symbols", [])]
        timeframes_cfg = [str(t) for t in config_data.get("timeframes", [])]
        HTF_cfg = [str(t) for t in config_data.get("HTF", [])]

        if not symbols_cfg or not timeframes_cfg:
            raise ValueError("Config must include at least one symbol and one timeframe")

        batch_size = int(config_data.get("backtest_batch_size", 1000))
        if batch_size <= 0:
            batch_size = 1000


        load_settings_from_db()
        public_settings.ExecuteID = 0 # it 's set to 0 for Live executions and Forward Tests; For Backtests, it will be set to the Backtest ID (positive integer) to isolate data per backtest run
        logger.info("Public settings loaded from database.")
        
        start_dt_raw = public_settings.Start_Date_Time
        end_dt_raw = public_settings.End_Date_Time

        print(f"Start_Date_Time: {start_dt_raw}, End_Date_Time: {end_dt_raw}")

        if not start_dt_raw or not end_dt_raw:
            raise ValueError("Config must include Start_Date_Time and End_Date_Time for backtest replay")

        start_dt = _parse_config_dt(start_dt_raw)
        end_dt = _parse_config_dt(end_dt_raw)
        if end_dt <= start_dt:
            raise ValueError("End_Date_Time must be greater than Start_Date_Time")


        #System initialization
        symbols = symbols_cfg
        timeframes = timeframes_cfg
        indicators = [str(t) for t in config_data.get("indicators", [])]  # e.g., ["ATR", "EMA_FAST", "EMA_SLOW", "inBands"]
        ps.symbol = symbols[0]

        #fill_indicator_gap()
        #logger.info("Indicator gap fill done.")

        #for main timeframe
        buffers.init_candle_buffer("OANDA", symbols, timeframes, until_time=start_dt)
        logger.info("Candle buffers initialized for OANDA symbols and timeframes.")
        buffers.init_indicator_buffer(symbols, timeframes, indicators)
        logger.info("Indicator buffers initialized for OANDA symbols and timeframes.")

        #for main timeframe
        buffers.init_candle_buffer("OANDA", symbols, HTF_cfg, until_time=start_dt)
        logger.info("Candle buffers initialized for OANDA symbols and HTF timeframes.")


        sm.open_sig_registry.bootstrap_from_db(DB_Conn, ps.symbol) 
        open_count = sm.open_sig_registry.get_count() 
        logger.info( json.dumps({ "EventCode": 0, "Message": f"open_sig_registry initialized. open_signals={open_count}" }) )
        #print("AAAAAAAAAAAAAAAA")
        #for sig in open_sig_registry.get_all_signals():
            #print(sig)
        #print("AAAAAAAAAAAAAAAA")
        # Print all open signals loaded from the DB for debugging/inspection
        


        '''
        # Print CANDLE_BUFFER stats for each symbol/timeframe
        for symbol in symbols:
            for timeframe in timeframes:
                key = Keys(exchange="OANDA", symbol=symbol, timeframe=timeframe)
                buf = buffers.CANDLE_BUFFER.get_or_create(key)
                count = len(buf)
                if count > 0:
                    last_candle = buf[-1]
                    first_candle = buf[0]
                    print(f"CANDLE_BUFFER[{symbol}/{timeframe}] count: {count}, last close_time: {last_candle.get('close_time')}, first close_time: {first_candle.get('close_time')}")
                else:
                    print(f"CANDLE_BUFFER[{symbol}/{timeframe}] is empty.")
        '''
        


        
        # Backtest replay loop: read DB candles in batches and feed engine one-by-one.
        total_processed = 0
        for symbol in symbols:
            for timeframe in timeframes:
                key = Keys(exchange="OANDA", symbol=symbol, timeframe=timeframe)
                last_close_time = None
                symbol_tf_count = 0

                while True:
                    candles = get_candles_in_range_batch(
                        key=key,
                        start_time=start_dt,
                        end_time=end_dt,
                        limit=batch_size,
                        after_close_time=last_close_time,
                    )
                    if not candles:
                        break

                    for candle in candles:
                        payload = _to_engine_payload(candle)
                        run_engine(payload)
                        symbol_tf_count += 1
                        total_processed += 1

                    last_close_time = candles[-1]["close_time"]
                    logger.info(
                        "Backtest replay progress %s %s: +%s candles (total=%s, last_close_time=%s)",
                        symbol,
                        timeframe,
                        len(candles),
                        symbol_tf_count,
                        last_close_time,
                    )

                logger.info(
                    "Backtest replay finished for %s %s. processed=%s, window=[%s, %s]",
                    symbol,
                    timeframe,
                    symbol_tf_count,
                    start_dt,
                    end_dt,
                )

        logger.info("Backtest replay complete. total_processed=%s", total_processed)

               
    finally:
        notify_telegram("⛔️ QF_RAEP_BT App stopped.", ChatType.ALERT)
        await close_telegram_notifier()


if __name__ == "__main__":
    asyncio.run(main())
