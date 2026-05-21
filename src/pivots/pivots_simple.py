# pivots_simple.py
# Minimal pivot (swing high/low) detector with plateau consolidation (LAST bar pick).
# Adds:
#   - open_time in outputs
#   - hit flag (strict or tolerance-based)
#   - MIN_PIVOT_DISTANCE filter
#   - symbol-aware pip-size + hit tolerance
#   - hit_distance = number of candles until pivot is hit (NEW & FIXED)

from typing import List, Dict, Tuple, Any, Optional
import numpy as np

PLATEAU_MAX_GAP = 1
MIN_PIVOT_DISTANCE = 5
HIT_TOLERANCE_PIPS_DEFAULT = 1.0
DEFAULT_PIP_SIZE = 0.0001


def _pip_size_for_symbol(symbol: str) -> float:
    s = symbol.replace("/", "").replace("_", "").upper()
    if len(s) >= 6:
        quote = s[-3:]
    else:
        return DEFAULT_PIP_SIZE

    return 0.01 if quote == "JPY" else DEFAULT_PIP_SIZE


def _enforce_min_pivot_distance(indices, values, *, is_peak, min_dist, eps):
    if min_dist is None or min_dist <= 1 or len(indices) == 0:
        return [int(i) for i in indices]

    kept = []
    for idx in indices:
        idx = int(idx)
        if not kept:
            kept.append(idx)
            continue

        last_idx = kept[-1]
        if idx - last_idx < min_dist:
            v_new = float(values[idx])
            v_old = float(values[last_idx])

            if is_peak:
                if v_new > v_old + eps:
                    kept[-1] = idx
            else:
                if v_new < v_old - eps:
                    kept[-1] = idx
        else:
            kept.append(idx)

    return kept


# ---------------------------------------------------------------
# NEW FIXED VERSION OF hit_distance (correct tolerance logic)
# ---------------------------------------------------------------
def _hit_distance(
    pivot_idx: int,
    *,
    is_peak: bool,
    H: np.ndarray,
    L: np.ndarray,
    n: int,
    hit_tolerance: float
) -> Optional[int]:

    start = pivot_idx + n + 1
    m = len(H)

    if start >= m:
        return None

    if is_peak:
        pivot_price = H[pivot_idx]
        threshold = pivot_price - hit_tolerance

        for j in range(start, m):
            if H[j] >= threshold:
                return j - pivot_idx

    else:
        pivot_price = L[pivot_idx]
        threshold = pivot_price + hit_tolerance

        for j in range(start, m):
            if L[j] <= threshold:
                return j - pivot_idx

    return None


# ---------------------------------------------------------------


def detect_pivots(
    candles: List[Dict[str, Any]],
    n: int = 5,
    eps: float = 1e-9,
    *,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    hit_tolerance_pips: float,
    high_key: str = "high",
    low_key: str = "low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    strict: bool = False,
    hit_strict: bool,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:

    m = len(candles)
    
    # Debug: print all candles
    #for i, c in enumerate(candles):
        #print(f"candles[{i}] = {c}")

    H = np.fromiter((c[high_key] for c in candles), float, count=m)
    L = np.fromiter((c[low_key] for c in candles), float, count=m)
    T_close = [c[time_key] for c in candles]
    T_open = [c.get(open_time_key) for c in candles]

    if m == 0 or m < 2 * n + 1:
        return [], []

    left, right = n, m - n
    h_mid = H[left:right]
    l_mid = L[left:right]

    if strict:
        cmp_hi_left = lambda a, b: a > b
        cmp_hi_right = lambda a, b: a > b
        cmp_lo_left = lambda a, b: a < b
        cmp_lo_right = lambda a, b: a < b
    else:
        cmp_hi_left = lambda a, b: a >= b
        cmp_hi_right = lambda a, b: a > b
        cmp_lo_left = lambda a, b: a <= b
        cmp_lo_right = lambda a, b: a < b

    peak_mask = np.ones(right - left, bool)
    low_mask = np.ones(right - left, bool)

    for k in range(1, n + 1):
        peak_mask &= (
            cmp_hi_left(h_mid, H[left - k:right - k])
            & cmp_hi_right(h_mid, H[left + k:right + k])
        )
        low_mask &= (
            cmp_lo_left(l_mid, L[left - k:right - k])
            & cmp_lo_right(l_mid, L[left + k:right + k])
        )

    is_peak = np.zeros(m, bool)
    is_low = np.zeros(m, bool)
    is_peak[left:right] = peak_mask
    is_low[left:right] = low_mask

    def consolidate(mask, vals):
        if eps is None:
            return mask
        out = np.zeros_like(mask, bool)
        i = 0
        while i < m:
            if not mask[i]:
                i += 1
                continue
            j = i
            while j + 1 < m and mask[j + 1] and abs(vals[j + 1] - vals[i]) <= eps:
                j += 1
            out[j] = True
            i = j + 1
        return out

    is_peak = consolidate(is_peak, H)
    is_low = consolidate(is_low, L)

    def _future_high_after(i):
        s = i + n + 1
        if s >= m:
            return float("-inf")
        return float(np.max(H[s:]))

    def _future_low_after(i):
        s = i + n + 1
        if s >= m:
            return float("inf")
        return float(np.min(L[s:]))

    if hit_strict:
        peak_hit_fn = lambda i: bool(_future_high_after(i) >= H[i])
        low_hit_fn = lambda i: bool(_future_low_after(i) <= L[i])
        hit_tol = 0.0
    else:
        if pip_size is None:
            pip_size = _pip_size_for_symbol(symbol) if symbol else DEFAULT_PIP_SIZE
        hit_tol = pip_size * hit_tolerance_pips

        peak_hit_fn = lambda i, tol=hit_tol: bool(_future_high_after(i) >= H[i] - tol)
        low_hit_fn = lambda i, tol=hit_tol: bool(_future_low_after(i) <= L[i] + tol)

    peak_idx = _enforce_min_pivot_distance(
        np.flatnonzero(is_peak), H, is_peak=True, min_dist=MIN_PIVOT_DISTANCE, eps=eps
    )
    low_idx = _enforce_min_pivot_distance(
        np.flatnonzero(is_low), L, is_peak=False, min_dist=MIN_PIVOT_DISTANCE, eps=eps
    )

    peaks = []
    for i in peak_idx:
        hit = peak_hit_fn(i)
        dist = _hit_distance(i, is_peak=True, H=H, L=L, n=n, hit_tolerance=hit_tol)
        peaks.append(
            {
                "index": int(i),
                "time": T_close[i],
                "open_time": T_open[i],
                "high": float(H[i]),
                "hit": hit,
                "hit_distance": dist,
            }
        )

    lows = []
    for i in low_idx:
        hit = low_hit_fn(i)
        dist = _hit_distance(i, is_peak=False, H=H, L=L, n=n, hit_tolerance=hit_tol)
        lows.append(
            {
                "index": int(i),
                "time": T_close[i],
                "open_time": T_open[i],
                "low": float(L[i]),
                "hit": hit,
                "hit_distance": dist,
            }
        )

    return peaks, lows


def detect_pivots_ver2(
    candles: List[Dict[str, Any]],
    n: int = 5,
    eps: float = 1e-9,
    *,
    symbol: Optional[str] = None,
    pip_size: Optional[float] = None,
    high_threshold_pips: float = 0.0,
    low_threshold_pips: float = 0.0,
    hit_tolerance_pips: float = HIT_TOLERANCE_PIPS_DEFAULT,
    high_key: str = "high",
    low_key: str = "low",
    time_key: str = "CloseTime",
    open_time_key: str = "OpenTime",
    hit_strict: bool = False,
    min_pivot_distance: int = MIN_PIVOT_DISTANCE,
    confirmation_threshold_pips: Optional[float] = None,
    require_confirmation: bool = True,
    enforce_alternation: bool = True,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    # Total number of candles available for pivot detection.
    total_candles = len(candles)
    if total_candles == 0 or total_candles < 2 * n + 1:
        return [], []

    # Equality tolerance for floating-point comparisons.
    equality_tolerance = 0.0 if eps is None else float(eps)

    # Resolve pip size from argument or symbol so thresholds in pips can be converted to price units.
    if pip_size is None:
        pip_size = _pip_size_for_symbol(symbol) if symbol else DEFAULT_PIP_SIZE

    # Convert thresholds from pips to absolute price deltas.
    high_threshold_price = float(high_threshold_pips) * float(pip_size)
    low_threshold_price = float(low_threshold_pips) * float(pip_size)
    hit_tolerance_price = float(hit_tolerance_pips) * float(pip_size)

    if confirmation_threshold_pips is None:
        confirmation_threshold_pips = max(float(high_threshold_pips), float(low_threshold_pips), 0.0)
    confirmation_threshold_price = float(confirmation_threshold_pips) * float(pip_size)

    # Extract candle fields as arrays/lists for fast index-based access.
    highs = np.fromiter((c[high_key] for c in candles), float, count=total_candles)
    lows = np.fromiter((c[low_key] for c in candles), float, count=total_candles)
    close_times = [c[time_key] for c in candles]
    open_times = [c.get(open_time_key) for c in candles]

    def _collect_high_plateau_candidates() -> List[Dict[str, Any]]:
        # Candidate list containing plateau boundaries and selected pick index.
        high_candidates: List[Dict[str, Any]] = []
        plateau_start_index = 0

        while plateau_start_index < total_candles:
            # Extend a contiguous flat-high plateau using equality tolerance.
            plateau_end_index = plateau_start_index
            while (
                plateau_end_index + 1 < total_candles
                and abs(highs[plateau_end_index + 1] - highs[plateau_start_index]) <= equality_tolerance
            ):
                plateau_end_index += 1

            # We need n candles outside the plateau on both sides.
            if plateau_start_index - n >= 0 and plateau_end_index + n < total_candles:
                pivot_high_price = float(highs[plateau_start_index])
                left_high_window = highs[plateau_start_index - n:plateau_start_index]
                right_high_window = highs[plateau_end_index + 1:plateau_end_index + 1 + n]

                # Every left/right candle must be at least threshold smaller than the pivot high.
                left_is_valid = bool(np.all(left_high_window <= pivot_high_price - high_threshold_price + equality_tolerance))
                right_is_valid = bool(np.all(right_high_window <= pivot_high_price - high_threshold_price + equality_tolerance))

                if left_is_valid and right_is_valid:
                    # For flat-top pivots, use the rightmost candle as the pivot pick.
                    right_pick_index = plateau_end_index
                    high_candidates.append(
                        {
                            "pick_index": int(right_pick_index),
                            "plateau_start": int(plateau_start_index),
                            "plateau_end": int(plateau_end_index),
                        }
                    )

            plateau_start_index = plateau_end_index + 1

        return high_candidates

    def _collect_low_plateau_candidates() -> List[Dict[str, Any]]:
        # Candidate list containing plateau boundaries and selected pick index.
        low_candidates: List[Dict[str, Any]] = []
        plateau_start_index = 0

        while plateau_start_index < total_candles:
            # Extend a contiguous flat-low plateau using equality tolerance.
            plateau_end_index = plateau_start_index
            while (
                plateau_end_index + 1 < total_candles
                and abs(lows[plateau_end_index + 1] - lows[plateau_start_index]) <= equality_tolerance
            ):
                plateau_end_index += 1

            # We need n candles outside the plateau on both sides.
            if plateau_start_index - n >= 0 and plateau_end_index + n < total_candles:
                pivot_low_price = float(lows[plateau_start_index])
                left_low_window = lows[plateau_start_index - n:plateau_start_index]
                right_low_window = lows[plateau_end_index + 1:plateau_end_index + 1 + n]

                # Every left/right candle must be at least threshold greater than the pivot low.
                left_is_valid = bool(np.all(left_low_window >= pivot_low_price + low_threshold_price - equality_tolerance))
                right_is_valid = bool(np.all(right_low_window >= pivot_low_price + low_threshold_price - equality_tolerance))

                if left_is_valid and right_is_valid:
                    # For flat-bottom pivots, use the rightmost candle as the pivot pick.
                    right_pick_index = plateau_end_index
                    low_candidates.append(
                        {
                            "pick_index": int(right_pick_index),
                            "plateau_start": int(plateau_start_index),
                            "plateau_end": int(plateau_end_index),
                        }
                    )

            plateau_start_index = plateau_end_index + 1

        return low_candidates

    # 1) Raw candidate extraction from local structure.
    high_candidates = _collect_high_plateau_candidates()
    low_candidates = _collect_low_plateau_candidates()

    # 2) Remove very-near duplicates in same direction.
    high_indices = _enforce_min_pivot_distance(
        [candidate["pick_index"] for candidate in high_candidates],
        highs,
        is_peak=True,
        min_dist=min_pivot_distance,
        eps=equality_tolerance,
    )
    low_indices = _enforce_min_pivot_distance(
        [candidate["pick_index"] for candidate in low_candidates],
        lows,
        is_peak=False,
        min_dist=min_pivot_distance,
        eps=equality_tolerance,
    )

    # Lookup maps from selected pick index to plateau boundaries.
    high_plateau_map = {candidate["pick_index"]: candidate for candidate in high_candidates}
    low_plateau_map = {candidate["pick_index"]: candidate for candidate in low_candidates}

    # 3) Build one time-ordered event stream so we can enforce H-L-H-L alternation.
    pivot_events: List[Dict[str, Any]] = []
    for pivot_index in high_indices:
        plateau_info = high_plateau_map.get(pivot_index, {"plateau_start": pivot_index, "plateau_end": pivot_index})
        pivot_events.append(
            {
                "kind": "H",
                "index": int(pivot_index),
                "price": float(highs[pivot_index]),
                "plateau_start": int(plateau_info["plateau_start"]),
                "plateau_end": int(plateau_info["plateau_end"]),
            }
        )

    for pivot_index in low_indices:
        plateau_info = low_plateau_map.get(pivot_index, {"plateau_start": pivot_index, "plateau_end": pivot_index})
        pivot_events.append(
            {
                "kind": "L",
                "index": int(pivot_index),
                "price": float(lows[pivot_index]),
                "plateau_start": int(plateau_info["plateau_start"]),
                "plateau_end": int(plateau_info["plateau_end"]),
            }
        )

    pivot_events.sort(key=lambda event: event["index"])

    if enforce_alternation:
        alternating_events: List[Dict[str, Any]] = []
        for event in pivot_events:
            if not alternating_events:
                alternating_events.append(event)
                continue

            previous_event = alternating_events[-1]
            if event["kind"] != previous_event["kind"]:
                alternating_events.append(event)
                continue

            # If two consecutive events are same kind, keep the stronger structural extreme.
            if event["kind"] == "H":
                if event["price"] > previous_event["price"] + equality_tolerance:
                    alternating_events[-1] = event
            else:
                if event["price"] < previous_event["price"] - equality_tolerance:
                    alternating_events[-1] = event
    else:
        alternating_events = pivot_events

    def _is_confirmed_by_opposite_move(event: Dict[str, Any]) -> bool:
        # Confirmation means price moved enough in the opposite direction after the pivot.
        start_index = int(event["index"]) + n + 1
        if start_index >= total_candles:
            return False

        event_price = float(event["price"])
        if event["kind"] == "H":
            future_low = float(np.min(lows[start_index:]))
            return future_low <= event_price - confirmation_threshold_price + equality_tolerance

        future_high = float(np.max(highs[start_index:]))
        return future_high >= event_price + confirmation_threshold_price - equality_tolerance

    # 4) Keep only confirmed events if confirmation filtering is enabled.
    if require_confirmation:
        selected_events = [event for event in alternating_events if _is_confirmed_by_opposite_move(event)]
    else:
        selected_events = alternating_events

    selected_high_indices = [int(event["index"]) for event in selected_events if event["kind"] == "H"]
    selected_low_indices = [int(event["index"]) for event in selected_events if event["kind"] == "L"]
    selected_high_plateau_map = {
        int(event["index"]): {
            "plateau_start": int(event["plateau_start"]),
            "plateau_end": int(event["plateau_end"]),
        }
        for event in selected_events
        if event["kind"] == "H"
    }
    selected_low_plateau_map = {
        int(event["index"]): {
            "plateau_start": int(event["plateau_start"]),
            "plateau_end": int(event["plateau_end"]),
        }
        for event in selected_events
        if event["kind"] == "L"
    }

    def _future_high_after(index: int) -> float:
        start_index = index + n + 1
        if start_index >= total_candles:
            return float("-inf")
        return float(np.max(highs[start_index:]))

    def _future_low_after(index: int) -> float:
        start_index = index + n + 1
        if start_index >= total_candles:
            return float("inf")
        return float(np.min(lows[start_index:]))

    if hit_strict:
        high_hit_checker = lambda index: bool(_future_high_after(index) >= highs[index])
        low_hit_checker = lambda index: bool(_future_low_after(index) <= lows[index])
        effective_hit_tolerance = 0.0
    else:
        high_hit_checker = lambda index, tol=hit_tolerance_price: bool(
            _future_high_after(index) >= highs[index] - tol
        )
        low_hit_checker = lambda index, tol=hit_tolerance_price: bool(
            _future_low_after(index) <= lows[index] + tol
        )
        effective_hit_tolerance = hit_tolerance_price

    detected_high_pivots: List[Dict[str, Any]] = []
    for pivot_index in selected_high_indices:
        plateau_info = selected_high_plateau_map.get(
            pivot_index,
            {"plateau_start": pivot_index, "plateau_end": pivot_index},
        )
        is_hit = high_hit_checker(pivot_index)
        candles_until_hit = _hit_distance(
            pivot_index,
            is_peak=True,
            H=highs,
            L=lows,
            n=n,
            hit_tolerance=effective_hit_tolerance,
        )
        detected_high_pivots.append(
            {
                "index": int(pivot_index),
                "time": close_times[pivot_index],
                "open_time": open_times[pivot_index],
                "high": float(highs[pivot_index]),
                "plateau_start": int(plateau_info["plateau_start"]),
                "plateau_end": int(plateau_info["plateau_end"]),
                "hit": is_hit,
                "hit_distance": candles_until_hit,
            }
        )

    detected_low_pivots: List[Dict[str, Any]] = []
    for pivot_index in selected_low_indices:
        plateau_info = selected_low_plateau_map.get(
            pivot_index,
            {"plateau_start": pivot_index, "plateau_end": pivot_index},
        )
        is_hit = low_hit_checker(pivot_index)
        candles_until_hit = _hit_distance(
            pivot_index,
            is_peak=False,
            H=highs,
            L=lows,
            n=n,
            hit_tolerance=effective_hit_tolerance,
        )
        detected_low_pivots.append(
            {
                "index": int(pivot_index),
                "time": close_times[pivot_index],
                "open_time": open_times[pivot_index],
                "low": float(lows[pivot_index]),
                "plateau_start": int(plateau_info["plateau_start"]),
                "plateau_end": int(plateau_info["plateau_end"]),
                "hit": is_hit,
                "hit_distance": candles_until_hit,
            }
        )

    return detected_high_pivots, detected_low_pivots
