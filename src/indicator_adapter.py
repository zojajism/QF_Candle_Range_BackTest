from typing import List, Dict, Any
from indicator_buffer import IndicatorKey, IndicatorBuffer

def _normalize_indicator(ind: Dict[str, Any]) -> Dict[str, Any]:
    # Accept multiple common variants; default to KeyError if missing
    value = ind.get("value")
    event_time = ind.get("event_time")
    if value is None or event_time is None:
        raise KeyError("Indicator is missing required fields (value/event_time).")
    return {
        "value": value,
        "event_time": event_time,
    }

def indicators_for_analysis(
    indicator_buffer: IndicatorBuffer,
    symbol: str,
    timeframe: str,
    key: str,
    n: int = 100
) -> List[Dict[str, Any]]:
    ind_key = IndicatorKey(symbol=symbol, timeframe=timeframe, key=key)
    raw = indicator_buffer.last_n(ind_key, n)
    return [_normalize_indicator(i) for i in raw]