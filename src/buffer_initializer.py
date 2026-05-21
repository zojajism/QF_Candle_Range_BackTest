# English-only comments

import datetime
from typing import List, Dict, Any
from candle_buffer import CandleBuffer, Keys
from db_general import get_candles_from_db, get_indicators_from_db, get_candles_from_db_offset
from indicator_buffer import IndicatorBuffer, IndicatorKey
import public_settings as ps

CANDLE_BUFFER: CandleBuffer = None
INDICATOR_BUFFER: IndicatorBuffer = None

def init_candle_buffer(exchange: str, symbols: List[str], timeframes: List[str], until_time: datetime.datetime) -> None:
    """
    Initialize global CandleBuffer.
    Load existing candles/indicators from DB if available.
    """
    global CANDLE_BUFFER

    # Candle capacity (per Keys)
    def candle_capacity_fn(key: Keys) -> int:
        return 300  

    # Create buffer once; keep existing candles across multiple init calls.
    if CANDLE_BUFFER is None:
        CANDLE_BUFFER = CandleBuffer(capacity_fn=candle_capacity_fn)

    # Preload from DB (optional)
    for symbol in symbols:
        for timeframe in timeframes:
            key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
            candles = get_candles_from_db(key, candle_capacity_fn(key), until_time)
            for candle in candles:
                CANDLE_BUFFER.append(key, candle)

    return CANDLE_BUFFER


def init_candle_buffer_Offset(exchange: str, symbols: List[str], timeframes: List[str]) -> None:
    """
    Initialize global CandleBuffer.
    Load existing candles/indicators from DB if available.
    """
    global CANDLE_BUFFER

    # Candle capacity (per Keys)
    def candle_capacity_fn(key: Keys) -> int:
        return 300  

    # Create buffer once; keep existing candles across multiple init calls.
    if CANDLE_BUFFER is None:
        CANDLE_BUFFER = CandleBuffer(capacity_fn=candle_capacity_fn)

    # Preload from DB (optional)
    for symbol in symbols:
        for timeframe in timeframes:
            key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)

            candles = get_candles_from_db_offset(key, candle_capacity_fn(key) - ps.Candle_Limit_offset, ps.Candle_Limit_offset)
            for candle in candles:
                CANDLE_BUFFER.append(key, candle)

    return CANDLE_BUFFER


def init_indicator_buffer(symbols: List[str], timeframes: List[str], keys: List[str]) -> IndicatorBuffer:
    global INDICATOR_BUFFER

    def indicator_capacity_fn(key: IndicatorKey) -> int:
        return 300  

    INDICATOR_BUFFER = IndicatorBuffer(capacity_fn=indicator_capacity_fn)

    for symbol in symbols:
        for timeframe in timeframes:
            for key in keys:
                indkey = IndicatorKey(symbol=symbol, timeframe=timeframe, key=key)
                indicators = get_indicators_from_db(indkey, indicator_capacity_fn(indkey))
                for ind in indicators:
                    INDICATOR_BUFFER.append(indkey, ind)
    return INDICATOR_BUFFER

