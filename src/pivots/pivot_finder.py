# src/pivots/pivot_finder.py

from typing import List, Tuple, Any, Dict, Optional
from .pivot_buffer import Pivot
from .pivots_simple import detect_pivots, detect_pivots_ver2

from dataclasses import dataclass
from decimal import Decimal
import public_moduls as pm

def compute_pivots(
    candles: List[Dict[str, Any]],
    *,
    n: int = 5,
    eps: float = 1e-9,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    hit_tolerance_pips: float,
    high_key: str = "High",
    low_key: str = "Low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool,
) -> Tuple[List[Pivot], List[Pivot]]:
    """
    Thin wrapper around detect_pivots() to map outputs into Pivot objects.

    Parameters
    ----------
    candles : list[dict]
        Input candles with at least High/Low and time fields.
    n : int
        Window size for pivot detection (number of bars on each side).
    eps : float
        Tolerance for plateau merging (price equality).
    symbol : str | None
        FX symbol (e.g. "EUR/USD", "USD_JPY") used to infer pip size if pip_size is None.
    pip_size : float | None
        Explicit pip size override. If None, detect_pivots will infer from symbol or use default.
    hit_tolerance_pips : float
        Hit tolerance in pips for non-strict hit mode (hit_strict=False).
    high_key, low_key, time_key, open_time_key : str
        Keys used to read values from each candle dict.
    strict : bool
        Pivot window strictness:
          - False: TV-like (left >= / <=, right > / <)
          - True : strict both sides (> / <)
    hit_strict : bool
        Hit evaluation strictness:
          - True : strictly > / < (no tolerance)
          - False: TV-like >= / <= with hit_tolerance_pips.
    """

    
    peaks_raw, lows_raw = detect_pivots(
        candles,
        n=n,
        eps=eps,
        symbol=symbol,
        pip_size=pip_size,
        hit_tolerance_pips=hit_tolerance_pips,
        high_key=high_key,
        low_key=low_key,
        time_key=time_key,
        open_time_key=open_time_key,
        strict=strict,
        hit_strict=hit_strict,
    )
    

    '''
    peaks_raw, lows_raw = detect_pivots_ver2(
        candles,
        n=n,
        eps=eps,
        symbol=symbol,
        pip_size=pip_size,
        high_threshold_pips=1.0,
        low_threshold_pips=1.0,
        hit_tolerance_pips=hit_tolerance_pips,
        high_key=high_key,
        low_key=low_key,
        time_key=time_key,
        open_time_key=open_time_key,
        hit_strict=hit_strict,
        min_pivot_distance=5.0,
    )
    '''

    peaks = [
        Pivot(
            time=p["time"],
            open_time=p.get("open_time"),
            price=p["high"],
            is_hit=bool(p.get("hit", False)),
            hit_distance=p.get("hit_distance"),   # <<< NEW
        )
        for p in peaks_raw
    ]

    lows = [
        Pivot(
            time=q["time"],
            open_time=q.get("open_time"),
            price=q["low"],
            is_hit=bool(q.get("hit", False)),
            hit_distance=q.get("hit_distance"),   # <<< NEW            
        )
        for q in lows_raw
    ]

    '''
    print("\nPeaks:")
    for p in peaks:
        print(
            f"  time={p.time}  price={p.price}  hit={p.is_hit}  hit_distance={p.hit_distance}"
        )

    print("\nLows:")
    for q in lows:
        print(
            f"  time={q.time}  price={q.price}  hit={q.is_hit}  hit_distance={q.hit_distance}"
        )
     '''      
    
    return peaks, lows



#================================================================================================================
# NEW implementation EXACTLY replicating TradingView's "Market Structure HH, HL, LH, LL" Pine Script (with zigzag_len=10)
#================================================================================================================

@dataclass
class MarketStructurePivot:
    pivot_type: str      # "HH", "HL", "LH", "LL"
    price: Decimal
    bar_index: int       # 0=oldest in candles list
    is_high: bool
    is_hit: bool
    open_time: Any
    close_time: Any


def detect_market_structure_pivots(
    candles: List[Dict[str, Any]], 
    zigzag_len: int = 5,
    dedupe_output: bool = True,
) -> List[MarketStructurePivot]:
    """
    TradingView-equivalent implementation of the provided Pine script.

        Notes:
    - This follows trend flips driven by rolling highest/lowest conditions.
    - Label events are emitted on trend change, including repeated labels when
      Pine would re-draw unchanged side labels on each change.
        - Set dedupe_output=True (default) for normal usage to remove repeated
            consecutive identical pivots from the returned list.
    """
    if not candles or zigzag_len < 1:
        return []

    def _barssince_last_true(series: List[bool]) -> Optional[int]:
        for i in range(len(series) - 1, -1, -1):
            if series[i]:
                return len(series) - 1 - i
        return None

    highs = [Decimal(str(c["high"])) for c in candles]
    lows = [Decimal(str(c["low"])) for c in candles]

    def _is_pivot_hit(pivot: MarketStructurePivot) -> bool:
        # A pivot is "hit" only if a later candle reaches/pierces its price.
        if pivot.bar_index >= len(candles) - 1:
            return False

        if pivot.is_high:
            return any(h >= pivot.price for h in highs[pivot.bar_index + 1 :])

        return any(l <= pivot.price for l in lows[pivot.bar_index + 1 :])

    trend = 1
    to_up_hist: List[bool] = []
    to_down_hist: List[bool] = []

    # Pine arrays are created with size=5, pre-filled with na.
    high_points: List[Optional[Decimal]] = [None] * 5
    high_index: List[Optional[int]] = [None] * 5
    low_points: List[Optional[Decimal]] = [None] * 5
    low_index: List[Optional[int]] = [None] * 5

    labeled_pivots: List[MarketStructurePivot] = []

    for bar_index in range(len(candles)):
        start = max(0, bar_index - zigzag_len + 1)
        to_up = highs[bar_index] >= max(highs[start : bar_index + 1])
        to_down = lows[bar_index] <= min(lows[start : bar_index + 1])
        to_up_hist.append(to_up)
        to_down_hist.append(to_down)

        prev_trend = trend
        trend = -1 if (trend == 1 and to_down) else (1 if (trend == -1 and to_up) else trend)
        trend_changed = trend != prev_trend

        shifted_to_up = to_up_hist[:-1]
        shifted_to_down = to_down_hist[:-1]
        last_trend_up_since = _barssince_last_true(shifted_to_up) if shifted_to_up else None
        last_trend_down_since = _barssince_last_true(shifted_to_down) if shifted_to_down else None

        low_len = last_trend_up_since if (last_trend_up_since is not None and last_trend_up_since > 0) else 1
        low_start = max(0, bar_index - low_len + 1)
        low_val = min(lows[low_start : bar_index + 1])
        low_idx = bar_index - next(offset for offset in range(bar_index + 1) if lows[bar_index - offset] == low_val)

        high_len = last_trend_down_since if (last_trend_down_since is not None and last_trend_down_since > 0) else 1
        high_start = max(0, bar_index - high_len + 1)
        high_val = max(highs[high_start : bar_index + 1])
        high_idx = bar_index - next(offset for offset in range(bar_index + 1) if highs[bar_index - offset] == high_val)

        if trend_changed:
            if trend == 1:
                low_points.append(low_val)
                low_index.append(low_idx)
            if trend == -1:
                high_points.append(high_val)
                high_index.append(high_idx)

            h0 = high_points[-1] if len(high_points) > 1 else None
            h0i = high_index[-1] if len(high_index) > 1 else None
            h1 = high_points[-2] if len(high_points) > 2 else None

            l0 = low_points[-1] if len(low_points) > 1 else None
            l0i = low_index[-1] if len(low_index) > 1 else None
            l1 = low_points[-2] if len(low_points) > 2 else None

            if h0 is not None and h1 is not None and h0i is not None:
                candle = candles[h0i]
                labeled_pivots.append(
                    MarketStructurePivot(
                        pivot_type="HH" if h0 > h1 else "LH",
                        price=h0,
                        bar_index=h0i,
                        is_high=True,
                        is_hit=False,
                        open_time=candle.get("open_time"),
                        close_time=candle.get("close_time"),
                    )
                )

            if l0 is not None and l1 is not None and l0i is not None:
                candle = candles[l0i]
                labeled_pivots.append(
                    MarketStructurePivot(
                        pivot_type="LL" if l0 < l1 else "HL",
                        price=l0,
                        bar_index=l0i,
                        is_high=False,
                        is_hit=False,
                        open_time=candle.get("open_time"),
                        close_time=candle.get("close_time"),
                    )
                )

    labeled_pivots.sort(key=lambda p: p.bar_index)

    for pivot in labeled_pivots:
        pivot.is_hit = _is_pivot_hit(pivot)

    if not dedupe_output:
        return labeled_pivots

    clean_pivots: List[MarketStructurePivot] = []
    last_key = None
    for pivot in labeled_pivots:
        key = (
            pivot.bar_index,
            pivot.pivot_type,
            pivot.is_high,
            pivot.price,
            pivot.open_time,
            pivot.close_time,
        )
        if key != last_key:
            clean_pivots.append(pivot)
            last_key = key

    return clean_pivots


def find_wave_extreme_with_time(
    candles: List[Dict], wave_start: int, wave_end: int, field: str, extreme_fn
) -> Tuple[Decimal, int, Any, Any]:
    """Find true extreme candle in wave - UNCHANGED"""
    extremes = []
    for i in range(wave_start, wave_end + 1):
        val = Decimal(candles[i][field])
        extremes.append((val, i, candles[i]["open_time"], candles[i]["close_time"]))
    
    extreme_val, extreme_idx, open_t, close_t = extreme_fn(extremes, key=lambda x: x[0])
    return extreme_val, extreme_idx, open_t, close_t

# Debug print (open_time DESC)
def print_pivots(pivots: List[MarketStructurePivot]):
    """Helper: print newest first"""
    '''
    print("=== ALL PIVOTS (newest first) ===")
    sorted_pivots = sorted(pivots, key=lambda p: p.open_time or 0, reverse=True)
    for p in sorted_pivots:
        print(f"{p.pivot_type}({p.price}) idx={p.bar_index} [{p.open_time}→{p.close_time}]")
    '''

    print("\nHIGH PIVOTS:")
    highs = sorted([p for p in pivots if p.is_high], key=lambda p: p.open_time or 0, reverse=True)
    for p in highs:
        print(f"{p.pivot_type}({round(p.price,5)}) at {p.bar_index} [{pm.format_time_simple(str(p.open_time))} ]")
    
    print("\nLOW PIVOTS:")
    lows = sorted([p for p in pivots if not p.is_high], key=lambda p: p.open_time or 0, reverse=True)
    for p in lows:
        print(f"{p.pivot_type}({round(p.price,5)}) at {p.bar_index} [{pm.format_time_simple(str(p.open_time))} ]")



def prepare_pivot_list_for_pivot_buffer(pivots: List[MarketStructurePivot]) -> Tuple[List[Pivot], List[Pivot]]:
    """Convert MarketStructurePivot list into simple BufferPivot list for PivotBuffer storage."""
        
    highs = sorted([p for p in pivots if p.is_high], key=lambda p: p.open_time or 0, reverse=True)
    lows = sorted([p for p in pivots if not p.is_high], key=lambda p: p.open_time or 0, reverse=True)

    peaks = [
        Pivot(
            time=p.close_time,
            open_time=p.open_time,
            price=round(p.price,5),
            is_hit=p.is_hit,
            hit_distance=0.0,
            type2=p.pivot_type,   
        )
        for p in highs
    ]

    lows = [
        Pivot(
            time=q.close_time,
            open_time=q.open_time,
            price=round(q.price,5),
            is_hit=q.is_hit,
            hit_distance=0.0,
            type2=q.pivot_type,   
        )
        for q in lows
    ]
    
    return peaks, lows