from dataclasses import dataclass
from collections import deque
from typing import Deque, Dict, Any, List, Tuple

@dataclass(frozen=True)
class IndicatorKey:
    symbol: str
    timeframe: str
    key: str

class IndicatorBuffer:
    def __init__(self, capacity_fn):
        self.map: Dict[IndicatorKey, Deque[Dict[str, Any]]] = {}
        self.capacity_fn = capacity_fn  # function: IndicatorKey -> int

    def get_or_create(self, ind_key: IndicatorKey) -> Deque[Dict[str, Any]]:
        dq = self.map.get(ind_key)
        if dq is None:
            dq = deque(maxlen=self.capacity_fn(ind_key))
            self.map[ind_key] = dq
        return dq

    def append(self, ind_key: IndicatorKey, indicator: Dict[str, Any]) -> None:
        dq = self.get_or_create(ind_key)
        dq.append(indicator)

    def last_n(self, ind_key: IndicatorKey, n: int) -> List[Dict[str, Any]]:
        dq = self.get_or_create(ind_key)
        if n <= 0:
            return []
        if n > len(dq):
            return list(dq)
        return list(dq)[-n:]

    def get_len(self, ind_key: IndicatorKey) -> int:
        dq = self.get_or_create(ind_key)
        return len(dq)