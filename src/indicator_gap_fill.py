import datetime
from decimal import Decimal
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

import buffer_initializer as buffers
from candle_buffer import Keys
from db_general import (
    get_last_available_indicator_time,
    get_N_last_candles_from_db,
    insert_strategy_modules_history,
)
from indicator_buffer import IndicatorKey
import public_settings as ps
import strategy_modules as sm
import public_moduls as pm 

logger = logging.getLogger(__name__)
INDICATOR_ANCHOR_KEY = "EMA_FAST"

def _parse_config_dt(value: str) -> datetime.datetime:
    value = str(value).strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed

def _load_runtime_config() -> tuple[List[str], List[str], List[str]]:
    config_path = Path("/data/config.yaml")
    if not config_path.exists():
        config_path = Path(__file__).resolve().parent / "data" / "config.yaml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file_obj:
        config_data = yaml.safe_load(file_obj) or {}

    symbols = [str(item) for item in config_data.get("symbols", [])]
    timeframes = [str(item) for item in config_data.get("timeframes", [])]
    indicators = [str(item) for item in config_data.get("indicators", [])]
    return symbols, timeframes, indicators

    

def _timeframe_to_timedelta(timeframe: str) -> datetime.timedelta:
    timeframe = timeframe.strip().lower()
    unit = timeframe[-1]
    value = int(timeframe[:-1])

    if unit == "m":
        return datetime.timedelta(minutes=value)
    if unit == "h":
        return datetime.timedelta(hours=value)
    if unit == "d":
        return datetime.timedelta(days=value)

    raise ValueError(f"Unsupported timeframe format: {timeframe}")

def indicator_update(candle_body: Dict[str, Any]):
    if isinstance(candle_body.get("open_time"), str):
        candle_body["open_time"] = datetime.datetime.fromisoformat(candle_body["open_time"])
    if isinstance(candle_body.get("close_time"), str):
        candle_body["close_time"] = datetime.datetime.fromisoformat(candle_body["close_time"])
    
    symbol = candle_body["symbol"]
    timeframe = candle_body["timeframe"] 
    exchange = candle_body["exchange"]   
    close_time = candle_body["close_time"]
    close = Decimal(str(round(candle_body["close"], 5)))

    sm.symbol = symbol
    sm.timeframe = timeframe
    sm.exchange = exchange
    sm.close_time = close_time

    logger.info(f"Candle received: {sm.symbol}, {sm.timeframe}, {pm.format_time_simple(str(sm.close_time))}, {close}")

    # Append candle to the list =============================================================================================
    key = Keys(sm.exchange, sm.symbol, sm.timeframe)
    buffers.CANDLE_BUFFER.append(key, candle_body)

    # Call engine modules to calculate engine parameters ====================================================================
    sm.calculate_ATR()
    sm.calculate_EMA(speed="fast")
    sm.calculate_EMA(speed="slow")
    sm.calculate_RSI()
    sm.check_close_in_Keltner_Bands()

    try:
        sm.calc_norm_slope()
    except Exception as e:
        logger.error(f"close_time: {close_time}, Error calculating normalized slope: {e}")    
    try:        
        sm.check_trend()
    except Exception as e:
        logger.error(f"close_time: {close_time}, Error determining trend: {e}")

    sm.is_bearish_engulfing_candle()
    sm.is_bullish_engulfing_candle()

    history_rows = [
        (close_time, symbol, timeframe, "ATR", sm.ATR),
        (close_time, symbol, timeframe, "EMA_FAST", sm.EMA_FAST),
        (close_time, symbol, timeframe, "EMA_SLOW", sm.EMA_SLOW),
        (close_time, symbol, timeframe, "inBands", sm.inBands),
        (close_time, symbol, timeframe, "EMA_Slope", sm.EMA_Slope),
        (close_time, symbol, timeframe, "upper_band", sm.upper_band),
        (close_time, symbol, timeframe, "lower_band", sm.lower_band),
        (close_time, symbol, timeframe, "Trend", sm.Trend),
        (close_time, symbol, timeframe, "Bullish_eng", sm.bullish_engulfing),
        (close_time, symbol, timeframe, "Bearish_eng", sm.bearish_engulfing),
        (close_time, symbol, timeframe, "RSI", sm.RSI),
    ]
    insert_strategy_modules_history(rows=history_rows)



def fill_indicator_gap():
    symbols, timeframes, indicators = _load_runtime_config()

    if not symbols or not timeframes:
        raise ValueError("Config must contain at least one symbol and one timeframe")

    symbol = symbols[0]
    timeframe = timeframes[0]
    
    buffers.init_indicator_buffer(symbols, timeframes, indicators)
     
    buffers.init_candle_buffer_Offset("OANDA", [symbol], [timeframe])
    logger.info("Candle buffers initialized with offset.")

    candle_key = Keys(exchange="OANDA", symbol=symbol, timeframe=timeframe)
    candle_deque = buffers.CANDLE_BUFFER.get_or_create(candle_key)
    #Print open_time and close for all candles currently loaded for this key.
    #print(f"Loaded {len(candle_deque)} candles for {symbol} {timeframe} with offset.")
    #i = 0
    #for candle in candle_deque:
        #i = i + 1
        #print(f"{i}: open_time={candle.get('open_time')} close={candle.get('close')}")

    # Load and replay candles newer than last_time to back-fill missing indicators.
    recent_candles = get_N_last_candles_from_db(candle_key, limit=40)
    #i = 0
    #for candle in reversed(recent_candles):
        #i = i + 1
        #print(f"{i}: open_time={candle.get('open_time')} close={candle.get('close')}")
    
    logger.info("Replaying 40 candles for indicator back-fill.")
    for candle in reversed(recent_candles):  
        indicator_update(candle)

    # Clear the full global candle buffer after printing.
    buffers.CANDLE_BUFFER.map.clear()
    buffers.INDICATOR_BUFFER.map.clear()
    logger.info("Global candle and indicator buffer cleared.")

    return
