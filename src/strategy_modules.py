from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
from datetime import datetime, timedelta, timezone
import logging
from os import close
from pathlib import Path
import re
from socket import close
import threading
from typing import List, Optional
import yaml
import public_settings as ps
import buffer_initializer
from candle_buffer import Keys
from db_general import insert_strategy_modules_history, get_pg_conn
import buffer_initializer as buffers
from indicator_buffer import IndicatorKey
from telegram_notifier import ChatType, notify_telegram
import public_moduls as pm
from pivots.pivot_buffer import PivotBufferRegistry, Pivot
from pivots.pivot_finder import compute_pivots, detect_market_structure_pivots, prepare_pivot_list_for_pivot_buffer, print_pivots
from pivots.pivot_registry_provider import get_pivot_registry
from signals import open_signal_registry

from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional


symbol = None
timeframe = None
exchange = None
close_time = None


ATR = None  
EMA_FAST = None  
EMA_SLOW = None  
K = None
inBands = None
EMA_Slope = None
is_up_trend = None
is_down_trend = None
Trend = None
RSI = None
ADX = None
PLUS_DI = None
MINUS_DI = None

upper_band = None
lower_band = None

bearish_engulfing = None
bullish_engulfing = None

Hammer = None
Shooting_Star = None
Three_Candle_Strike_Bullish = None
Three_Candle_Strike_Bearish = None

CE_LONG = None
CE_SHORT = None
Volume_AVG = None

MACD_LINE = None
MACD_SIGNAL = None
MACD_HISTOGRAM = None
MACD_HISTOGRAM_EXPANDING = None

DEFAULT_ORDER_UNITS = None

Last_Signal_Date = None
Last_Signal_Date_Counter = 0

#Higher Timeframe (HTF) management variables
HTF_candle_bios = None
Candle_Count_After_HTF_Reset = 0

#strategy_modules_history_rows = []  # type: List[tuple]

open_sig_registry = open_signal_registry.get_open_signal_registry()

logger = logging.getLogger(__name__)

_HTF_RUNTIME = None


def _resolve_config_path() -> Path:
    config_path = Path("/data/config.yaml")
    if config_path.exists():
        return config_path
    return Path(__file__).resolve().parent / "data" / "config.yaml"


def _timeframe_to_seconds(tf: str) -> int:
    token = str(tf).strip().lower()
    m = re.fullmatch(r"(\d+)\s*([mhdw])", token)
    if not m:
        raise ValueError(f"Unsupported timeframe format: {tf}")

    value = int(m.group(1))
    unit = m.group(2)

    if unit == "m":
        return value * 60
    if unit == "h":
        return value * 3600
    if unit == "d":
        return value * 86400
    if unit == "w":
        return value * 604800

    raise ValueError(f"Unsupported timeframe unit: {tf}")


def _normalize_dt(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


FOREX_MARKET_START_HOUR_UTC = 21  # Sunday 21:00 UTC


def _get_seconds_since_forex_market_start(dt: datetime) -> int:
    """
    Calculate seconds elapsed since the last Forex market start (21:00 UTC).
    Forex week begins Sunday 21:00 UTC, and this function measures time
    relative to that rolling boundary across all days.
    """
    utc_dt = _normalize_dt(dt)

    # Start of current day at 00:00 UTC
    day_start = utc_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    # Seconds elapsed into the current calendar day
    seconds_in_day = int((utc_dt - day_start).total_seconds())

    # Forex market start: 21:00 UTC = 75600 seconds into a calendar day
    market_start_seconds = FOREX_MARKET_START_HOUR_UTC * 3600  # 75600

    if seconds_in_day >= market_start_seconds:
        # We are in today\'s Forex market session (started at 21:00 today)
        return seconds_in_day - market_start_seconds
    else:
        # We are in yesterday\'s Forex market session (which started at 21:00 yesterday)
        # Calculate: seconds remaining from yesterday after 21:00 + seconds elapsed in today
        seconds_from_yesterday = (24 * 3600) - market_start_seconds
        return seconds_from_yesterday + seconds_in_day

def Valid_Signal_Counter(close_time: datetime) -> bool:
    global Last_Signal_Date, Last_Signal_Date_Counter

    signal_date = _normalize_dt(close_time).date()

    # First valid signal ever, or first valid signal of a new UTC date.
    if Last_Signal_Date is None or signal_date != Last_Signal_Date:
        Last_Signal_Date = signal_date
        Last_Signal_Date_Counter = 1
        return True

    # Same date: allow at most 2 valid signals in total.
    if Last_Signal_Date_Counter >= 2:
        return False

    Last_Signal_Date_Counter += 1
    return True

def _load_htf_runtime() -> dict | None:
    global _HTF_RUNTIME

    if _HTF_RUNTIME is not None:
        return _HTF_RUNTIME

    try:
        cfg_path = _resolve_config_path()
        if not cfg_path.exists():
            logger.warning("manage_HTF skipped: config.yaml not found at %s", cfg_path)
            _HTF_RUNTIME = {}
            return _HTF_RUNTIME

        with cfg_path.open("r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}

        ltf_list = [str(item).strip() for item in cfg.get("timeframes", []) if str(item).strip()]
        htf_list = [str(item).strip() for item in cfg.get("HTF", []) if str(item).strip()]

        if len(ltf_list) != 1 or len(htf_list) != 1:
            logger.warning(
                "manage_HTF expects exactly one timeframe and one HTF in config. timeframes=%s HTF=%s",
                ltf_list,
                htf_list,
            )
            _HTF_RUNTIME = {}
            return _HTF_RUNTIME

        ltf_tf = ltf_list[0]
        htf_tf = htf_list[0]
        ltf_sec = _timeframe_to_seconds(ltf_tf)
        htf_sec = _timeframe_to_seconds(htf_tf)

        if htf_sec <= ltf_sec or (htf_sec % ltf_sec) != 0:
            logger.warning(
                "manage_HTF invalid config: HTF (%s) must be a multiple of timeframe (%s)",
                htf_tf,
                ltf_tf,
            )
            _HTF_RUNTIME = {}
            return _HTF_RUNTIME

        count_override = cfg.get("HTF_candles_count")
        ratio = htf_sec // ltf_sec
        if count_override is not None and str(count_override).strip().lower() != "auto":
            try:
                ratio_override = int(count_override)
                if ratio_override > 0 and (ratio_override * ltf_sec) == htf_sec:
                    ratio = ratio_override
                else:
                    logger.warning(
                        "Ignoring HTF_candles_count=%s because it does not match %s -> %s",
                        count_override,
                        ltf_tf,
                        htf_tf,
                    )
            except Exception:
                logger.warning("Ignoring non-numeric HTF_candles_count=%s", count_override)

        _HTF_RUNTIME = {
            "ltf_tf": ltf_tf,
            "htf_tf": htf_tf,
            "ltf_sec": ltf_sec,
            "htf_sec": htf_sec,
            "ratio": ratio,
        }
        logger.info(
            "HTF runtime ready: %s -> %s (ratio=%s)",
            ltf_tf,
            htf_tf,
            ratio,
        )
        return _HTF_RUNTIME

    except Exception as e:
        logger.error("manage_HTF config load error: %s", e)
        _HTF_RUNTIME = {}
        return _HTF_RUNTIME

def calculate_Volume_AVG():
    # TradingView Vol uses a simple moving average with configurable MA Length.
    VOLUME_MA_LENGTH: int = 20

    global Volume_AVG
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, VOLUME_MA_LENGTH)

    if len(candles) < VOLUME_MA_LENGTH:
        raise ValueError("Not enough candles to calculate Volume AVG")
    
    total_volume = sum(Decimal(str(c["volume"])) for c in candles)
    Volume_AVG = (total_volume / Decimal(len(candles))).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

    ind_key = IndicatorKey(symbol, timeframe, "VOLUME_AVG")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Volume_AVG})

def calculate_MACD():
    """
    TradingView-style MACD with improved precision:
      - Fast EMA: 12
      - Slow EMA: 26
      - Signal EMA: 9 (applied to MACD line)
    
    Uses deep historical warm-up (all available candles) for proper EMA convergence,
    matching TradingView's calculation method.
    """
    global MACD_LINE, MACD_SIGNAL, MACD_HISTOGRAM, MACD_HISTOGRAM_EXPANDING

    fast_len = 12
    slow_len = 26
    signal_len = 9

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    
    # Use ALL available candles for proper EMA warm-up (TradingView method)
    available_candles = buffer_initializer.CANDLE_BUFFER.get_len(key)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, available_candles)

    # Need substantial warm-up for proper EMA convergence to match TradingView
    # Minimum: slow_len + signal_len, but recommend 50+ for convergence
    min_required = max(slow_len + signal_len, 50)
    if len(candles) < min_required:
        raise ValueError(f"Not enough candles to calculate MACD (have {len(candles)}, need {min_required})")

    closes = [Decimal(str(c["close"])) for c in candles]

    def _ema_series_tv_style(values: List[Decimal], period: int) -> List[Optional[Decimal]]:
        """
        TradingView-style EMA with Wilder smoothing convergence.
        Uses deep history seed for proper initialization.
        """
        series: List[Optional[Decimal]] = [None] * len(values)
        if len(values) < period:
            return series

        multiplier = Decimal("2") / Decimal(period + 1)
        
        # Seed: SMA of first 'period' bars for initial EMA
        ema = sum(values[:period]) / Decimal(period)
        series[period - 1] = ema

        # Apply EMA formula for remaining bars (Wilder smoothing)
        for i in range(period, len(values)):
            ema = (values[i] - ema) * multiplier + ema
            series[i] = ema

        return series

    # Calculate fast and slow EMAs across full history
    fast_ema = _ema_series_tv_style(closes, fast_len)
    slow_ema = _ema_series_tv_style(closes, slow_len)

    # Build MACD line (fast EMA - slow EMA) for all valid bars
    macd_line_full: List[Optional[Decimal]] = []
    for idx in range(len(closes)):
        if fast_ema[idx] is not None and slow_ema[idx] is not None:
            macd_line_full.append(fast_ema[idx] - slow_ema[idx])
        else:
            macd_line_full.append(None)

    # Extract only the non-None MACD values for signal line calculation
    macd_values_clean = [v for v in macd_line_full if v is not None]

    if len(macd_values_clean) < signal_len:
        raise ValueError(f"Not enough MACD values for signal (have {len(macd_values_clean)}, need {signal_len})")

    # Calculate signal line (9-period EMA of MACD line)
    signal_series = _ema_series_tv_style(macd_values_clean, signal_len)

    # Get current bar values
    macd_last = macd_values_clean[-1]
    signal_last = signal_series[-1]
    
    if signal_last is None:
        raise ValueError("Unable to calculate MACD signal")

    hist_last = macd_last - signal_last

    # Quantize to 5 decimals for precision
    MACD_LINE = macd_last.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    MACD_SIGNAL = signal_last.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    MACD_HISTOGRAM = hist_last.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

    # --- Histogram expansion & sign change detection ---
    # Detects two key conditions:
    #   1. Sign change (color flip): histogram changes from positive to negative or vice versa
    #      → Indicates potential trend reversal, beginning of opposite trend
    #   2. Magnitude expansion: histogram growing in absolute value while maintaining sign
    #      → Indicates trend strengthening/continuing
    hist_ind_key = IndicatorKey(symbol, timeframe, "MACD_HISTOGRAM")
    prev_hist_entries = buffers.INDICATOR_BUFFER.last_n(hist_ind_key, 2)
    
    if len(prev_hist_entries) >= 1:
        prev_hist_val = Decimal(str(prev_hist_entries[-1]["value"]))
        curr_hist_val = MACD_HISTOGRAM
        
        # Check for sign change (color change in histogram)
        sign_changed = (prev_hist_val > 0 and curr_hist_val < 0) or (prev_hist_val < 0 and curr_hist_val > 0)
        
        # Check for magnitude expansion (growing in absolute value)
        magnitude_expanding = abs(curr_hist_val) > abs(prev_hist_val)
        
        # HISTOGRAM_EXPANDING = 1 if sign changed OR magnitude is expanding
        # Sign change is the primary signal (trend reversal beginning)
        MACD_HISTOGRAM_EXPANDING = 1 if (sign_changed or magnitude_expanding) else 0
    else:
        MACD_HISTOGRAM_EXPANDING = 0

    ind_key = IndicatorKey(symbol, timeframe, "MACD_LINE")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": MACD_LINE})

    ind_key = IndicatorKey(symbol, timeframe, "MACD_SIGNAL")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": MACD_SIGNAL})

    ind_key = IndicatorKey(symbol, timeframe, "MACD_HISTOGRAM")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": MACD_HISTOGRAM})

    ind_key = IndicatorKey(symbol, timeframe, "MACD_HISTOGRAM_EXPANDING")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": MACD_HISTOGRAM_EXPANDING})

def calculate_ATR(timeframe: str):
    global ATR
    ATR_LEN = ps.ATR_LEN
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    # ATR needs one extra candle to compute N true-range values.
    warmup_candles = max((ATR_LEN * 5) + 1, ATR_LEN + 1)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, warmup_candles)
    if len(candles) < ATR_LEN + 1:
        raise ValueError("Not enough candles to calculate ATR")

    trs: List[Decimal] = []
    for i in range(1, len(candles)):
        high = Decimal(str(candles[i]["high"]))
        low = Decimal(str(candles[i]["low"]))
        prev_close = Decimal(str(candles[i - 1]["close"]))
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    # Seed ATR with the first N true-range values, then apply Wilder smoothing.
    ATR = sum(trs[:ATR_LEN]) / Decimal(ATR_LEN)
    for tr in trs[ATR_LEN:]:
        ATR = ((ATR * Decimal(ATR_LEN - 1)) + tr) / Decimal(ATR_LEN)

    # --- Round to 5 decimal places ---
    ATR = ATR.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    
    # Append ATR to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, "ATR")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": ATR})

def calculate_EMA(speed: str):

    if speed.lower() == "fast":
        period = ps.EMA_FAST_LEN
    elif speed.lower() == "slow":
        period = ps.EMA_SLOW_LEN
    else:
        raise ValueError("speed must be 'fast' or 'slow'")

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)

    # Need at least period candles + seed buffer
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, period * 3)

    if len(candles) < period:
        raise ValueError("Not enough candles to calculate EMA")

    # --- Extract closes as Decimal ---
    closes = [Decimal(str(c["close"])) for c in candles]

    # --- Multiplier ---
    multiplier = Decimal("2") / Decimal(period + 1)

    # --- Initial EMA (SMA seed) ---
    sma = sum(closes[:period]) / Decimal(period)
    ema = sma

    # --- EMA Iteration ---
    for price in closes[period:]:
        ema = (price - ema) * multiplier + ema

    # --- Round to 5 decimal places ---
    ema = ema.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

    if speed.lower() == "fast":
        global EMA_FAST
        EMA_FAST = ema
        _key = "EMA_FAST"
    elif speed.lower() == "slow":
        global EMA_SLOW
        EMA_SLOW = ema  
        _key = "EMA_SLOW"

    # Append EMA to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, _key)
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": ema})

def calculate_RSI():
    global RSI
    RSI_LEN = 14
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    
    # RSI is very sensitive to initialization; use deep history to match TradingView.
    # Wilder smoothing with more iterations converges much closer to TV values.
    available_candles = buffer_initializer.CANDLE_BUFFER.get_len(key)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, available_candles)
    
    if len(candles) < RSI_LEN + 1:
        raise ValueError("Not enough candles to calculate RSI")

    # Extract closes as Decimal
    closes = [Decimal(str(c["close"])) for c in candles]
    
    # Calculate gains and losses
    gains: List[Decimal] = []
    losses: List[Decimal] = []
    
    for i in range(1, len(closes)):
        change = closes[i] - closes[i - 1]
        if change > 0:
            gains.append(change)
            losses.append(Decimal("0"))
        else:
            gains.append(Decimal("0"))
            losses.append(abs(change))
    
    # Seed average gain and average loss with the first RSI_LEN values
    avg_gain = sum(gains[:RSI_LEN]) / Decimal(RSI_LEN)
    avg_loss = sum(losses[:RSI_LEN]) / Decimal(RSI_LEN)
    
    # Apply Wilder's smoothing for remaining values
    for i in range(RSI_LEN, len(gains)):
        avg_gain = ((avg_gain * Decimal(RSI_LEN - 1)) + gains[i]) / Decimal(RSI_LEN)
        avg_loss = ((avg_loss * Decimal(RSI_LEN - 1)) + losses[i]) / Decimal(RSI_LEN)
    
    # Calculate RS and RSI
    if avg_loss == 0:
        RSI = Decimal("100") if avg_gain > 0 else Decimal("50")
    else:
        rs = avg_gain / avg_loss
        RSI = Decimal("100") - (Decimal("100") / (Decimal("1") + rs))
    
    # --- Round to 5 decimal places ---
    RSI = RSI.quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    
    # Append RSI to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, "RSI")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": RSI})


def calculate_Chandelier_Exit():
    """
    Chandelier Exit — matches TradingView's built-in indicator.

    Long Exit  = Highest High over CE_LENGTH bars  -  ATR(CE_ATR_LENGTH) * CE_MULTIPLIER
    Short Exit = Lowest Low  over CE_LENGTH bars   +  ATR(CE_ATR_LENGTH) * CE_MULTIPLIER

    ATR is computed with Wilder/RMA smoothing (same as TradingView).
    """
    global CE_LONG, CE_SHORT

    # ---- Settings (fill from config when ready) ----
    CE_LENGTH: int = 22          # lookback for Highest High / Lowest Low
    CE_ATR_LENGTH: int = 22      # ATR period (Wilder / RMA)
    CE_MULTIPLIER: Decimal = Decimal("3")  # ATR multiplier
    # ------------------------------------------------

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)

    # Need CE_LENGTH candles for the rolling window plus CE_ATR_LENGTH + 1
    # for the ATR seed, so request enough history to cover both.
    warmup_candles = max((CE_ATR_LENGTH * 5) + 1, CE_LENGTH + CE_ATR_LENGTH + 1)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, warmup_candles)

    if len(candles) < CE_ATR_LENGTH + 1:
        raise ValueError("Not enough candles to calculate Chandelier Exit")

    # ---- True Range series ----
    trs: List[Decimal] = []
    for i in range(1, len(candles)):
        high = Decimal(str(candles[i]["high"]))
        low = Decimal(str(candles[i]["low"]))
        prev_close = Decimal(str(candles[i - 1]["close"]))
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        trs.append(tr)

    # ---- ATR via Wilder / RMA smoothing ----
    ce_atr = sum(trs[:CE_ATR_LENGTH]) / Decimal(CE_ATR_LENGTH)
    for tr in trs[CE_ATR_LENGTH:]:
        ce_atr = ((ce_atr * Decimal(CE_ATR_LENGTH - 1)) + tr) / Decimal(CE_ATR_LENGTH)

    # ---- Rolling Highest High and Lowest Low over CE_LENGTH bars ----
    # candles[-CE_LENGTH:] gives the most recent CE_LENGTH closed candles.
    recent = candles[-CE_LENGTH:]
    highest_high = max(Decimal(str(c["high"])) for c in recent)
    lowest_low = min(Decimal(str(c["low"])) for c in recent)

    # ---- Chandelier Exit lines ----
    CE_LONG = (highest_high - CE_MULTIPLIER * ce_atr).quantize(
        Decimal("0.00001"), rounding=ROUND_HALF_UP
    )
    CE_SHORT = (lowest_low + CE_MULTIPLIER * ce_atr).quantize(
        Decimal("0.00001"), rounding=ROUND_HALF_UP
    )

    # ---- Store in Indicator Buffer ----
    ind_key = IndicatorKey(symbol, timeframe, "CE_LONG")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": CE_LONG})

    ind_key = IndicatorKey(symbol, timeframe, "CE_SHORT")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": CE_SHORT})

def calculate_ADX(timeframe: str):
    global ADX, PLUS_DI, MINUS_DI

    ADX_LEN = 14

    key = Keys(
        exchange=exchange,
        symbol=symbol,
        timeframe=timeframe,
    )

    # ADX is very sensitive to initialization.
    # Use large warmup to match TradingView more closely.
    warmup_candles = max(300, (ADX_LEN * 20) + 1)

    candles = buffer_initializer.CANDLE_BUFFER.last_n(
        key,
        warmup_candles,
    )

    # Need at least len + 1 candles
    if len(candles) < (ADX_LEN + 1):
        raise ValueError("Not enough candles to calculate ADX")

    trs: List[Decimal] = []
    plus_dms: List[Decimal] = []
    minus_dms: List[Decimal] = []

    # ---------------------------------------------------------
    # Build TR, +DM, -DM series
    # ---------------------------------------------------------

    for i in range(1, len(candles)):

        curr_high = Decimal(str(candles[i]["high"]))
        curr_low = Decimal(str(candles[i]["low"]))

        prev_high = Decimal(str(candles[i - 1]["high"]))
        prev_low = Decimal(str(candles[i - 1]["low"]))
        prev_close = Decimal(str(candles[i - 1]["close"]))

        # True Range
        tr = max(
            curr_high - curr_low,
            abs(curr_high - prev_close),
            abs(curr_low - prev_close),
        )

        trs.append(tr)

        # Directional Movement
        up_move = curr_high - prev_high
        down_move = prev_low - curr_low

        plus_dm = (
            up_move
            if (up_move > down_move and up_move > 0)
            else Decimal("0")
        )

        minus_dm = (
            down_move
            if (down_move > up_move and down_move > 0)
            else Decimal("0")
        )

        plus_dms.append(plus_dm)
        minus_dms.append(minus_dm)

    # ---------------------------------------------------------
    # Wilder seed values (first 14)
    # ---------------------------------------------------------

    smoothed_tr = sum(trs[:ADX_LEN])

    smoothed_plus_dm = sum(plus_dms[:ADX_LEN])

    smoothed_minus_dm = sum(minus_dms[:ADX_LEN])

    dx_values: List[Decimal] = []

    # ---------------------------------------------------------
    # First DI/DX
    # ---------------------------------------------------------

    def calculate_di_dx(
        tr_value: Decimal,
        plus_dm_value: Decimal,
        minus_dm_value: Decimal,
    ):

        if tr_value == 0:
            return (
                Decimal("0"),
                Decimal("0"),
                Decimal("0"),
            )

        plus_di = (
            plus_dm_value / tr_value
        ) * Decimal("100")

        minus_di = (
            minus_dm_value / tr_value
        ) * Decimal("100")

        di_sum = plus_di + minus_di

        if di_sum == 0:
            return (
                plus_di,
                minus_di,
                Decimal("0"),
            )

        dx = (
            abs(plus_di - minus_di) / di_sum
        ) * Decimal("100")

        return (
            plus_di,
            minus_di,
            dx,
        )

    PLUS_DI, MINUS_DI, first_dx = calculate_di_dx(
        smoothed_tr,
        smoothed_plus_dm,
        smoothed_minus_dm,
    )

    dx_values.append(first_dx)

    # ---------------------------------------------------------
    # Wilder smoothing loop
    # ---------------------------------------------------------

    for i in range(ADX_LEN, len(trs)):

        smoothed_tr = (
            smoothed_tr
            - (smoothed_tr / Decimal(ADX_LEN))
            + trs[i]
        )

        smoothed_plus_dm = (
            smoothed_plus_dm
            - (smoothed_plus_dm / Decimal(ADX_LEN))
            + plus_dms[i]
        )

        smoothed_minus_dm = (
            smoothed_minus_dm
            - (smoothed_minus_dm / Decimal(ADX_LEN))
            + minus_dms[i]
        )

        PLUS_DI, MINUS_DI, dx = calculate_di_dx(
            smoothed_tr,
            smoothed_plus_dm,
            smoothed_minus_dm,
        )

        dx_values.append(dx)

    # ---------------------------------------------------------
    # ADX calculation
    # ---------------------------------------------------------

    if len(dx_values) < ADX_LEN:
        ADX = Decimal("0")

    else:

        # First ADX = SMA of first 14 DX values
        adx = (
            sum(dx_values[:ADX_LEN])
            / Decimal(ADX_LEN)
        )

        # Wilder smoothing for remaining ADX values
        for dx in dx_values[ADX_LEN:]:

            adx = (
                (
                    adx * Decimal(ADX_LEN - 1)
                ) + dx
            ) / Decimal(ADX_LEN)

        ADX = adx

    # ---------------------------------------------------------
    # Final rounding
    # ---------------------------------------------------------

    PLUS_DI = PLUS_DI.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )

    MINUS_DI = MINUS_DI.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )

    ADX = ADX.quantize(
        Decimal("0.01"),
        rounding=ROUND_HALF_UP,
    )

    # ---------------------------------------------------------
    # Store indicators
    # ---------------------------------------------------------

    ind_key = IndicatorKey(
        symbol,
        timeframe,
        "PLUS_DI",
    )

    buffers.INDICATOR_BUFFER.append(
        ind_key,
        {
            "event_time": close_time,
            "value": PLUS_DI,
        },
    )

    ind_key = IndicatorKey(
        symbol,
        timeframe,
        "MINUS_DI",
    )

    buffers.INDICATOR_BUFFER.append(
        ind_key,
        {
            "event_time": close_time,
            "value": MINUS_DI,
        },
    )

    ind_key = IndicatorKey(
        symbol,
        timeframe,
        "ADX",
    )

    buffers.INDICATOR_BUFFER.append(
        ind_key,
        {
            "event_time": close_time,
            "value": ADX,
        },
    )

def check_close_in_Keltner_Bands():

    global inBands, upper_band, lower_band

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)

    high = Decimal(str(candle[0]["high"]))
    low = Decimal(str(candle[0]["low"]))

    if ATR is None or EMA_FAST is None:
        raise ValueError("ATR and EMA_FAST must be calculated first")

    K_mult = ps.K_mult
    middle_band = EMA_FAST
    upper_band = (EMA_FAST + (K_mult * ATR)).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
    lower_band = (EMA_FAST - (K_mult * ATR)).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)

    if Trend == 1: 
        if low >= lower_band and low <= upper_band:
            inBands = 1
        else:        
            inBands = 0
    
    if Trend == -1: 
        if high >= lower_band and high <= upper_band:
            inBands = 1
        else:        
            inBands = 0

    # Append inBands to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, "inBands")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": inBands})

def calc_norm_slope():
    ind_key = IndicatorKey(symbol, timeframe, "EMA_FAST")
    EMAs = buffer_initializer.INDICATOR_BUFFER.last_n(ind_key, ps.K_Slope + 1)

    global EMA_Slope
    try:
        EMA_Slope = Decimal((EMA_FAST - Decimal(EMAs[0]["value"])) / (ps.K_Slope * ATR))
        EMA_Slope = EMA_Slope.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)  
    except Exception as e:
        EMA_Slope = Decimal("0.0")
        logger.error(f"EMA_Slope error: {close_time}, EMA_FAST: {EMA_FAST}, EMA_FAT_20: {EMAs[0]['value']}, ATR: {ATR} Setting EMA_Slope to 0.0. Error: {e}")
        '''
        notify_telegram(f"EMA_Slope error:   \n "
                         f"{pm.format_time_simple(str(close_time))}\n"
                         f"EMA_FAST: {EMA_FAST}\n"
                         f"EMA_FAT_20: {EMAs[0]['value']}\n"
                         f"ATR: {ATR}\n"
                         f"Error: {e}", ChatType.ALERT)
        '''

    if EMA_Slope is None:
        EMA_Slope = Decimal("0.0")

    # Append EMA_Slope to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, "EMA_Slope")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": EMA_Slope})


def check_up_trend():

    # check to see if Slope of EMA is above threshold for M consecutive bars
    ind_key = IndicatorKey(symbol, timeframe, "EMA_Slope")
    EMA_Slopes = buffer_initializer.INDICATOR_BUFFER.last_n(ind_key, ps.M_Slope)
    
    TrueSlope_candles_count = 0
    for i in range(len(EMA_Slopes)):
        if EMA_Slopes[i]["value"] > ps.S_th:
            TrueSlope_candles_count += 1
    is_slope_confirmed = (TrueSlope_candles_count == ps.M_Slope)

    # Check how many candles are above EMA in last K candles
    TrendCandles_count = 0
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, ps.K_Slope)
    EMA_key = IndicatorKey(symbol, timeframe, "EMA_FAST")
    EMAs = buffer_initializer.INDICATOR_BUFFER.last_n(EMA_key, ps.K_Slope)
    
    try:
        for j in range(len(candles)):
            if candles[j]["open"] > EMAs[j]["value"] and candles[j]["close"] > EMAs[j]["value"]:
                TrendCandles_count += 1
        is_Candles_with_trend_confirmed = (TrendCandles_count >= ps.N_Trend_Candle)

        if (is_slope_confirmed or ps.ignore_slope == 1) and is_Candles_with_trend_confirmed:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error in check_up_trend: {e}")
        return False
    
def check_down_trend():

    # check to see if Slope of EMA is below threshold for M consecutive bars
    ind_key = IndicatorKey(symbol, timeframe, "EMA_Slope")
    EMA_Slopes = buffer_initializer.INDICATOR_BUFFER.last_n(ind_key, ps.M_Slope)
    
    TrueSlope_candles_count = 0
    for i in range(len(EMA_Slopes)):
        if EMA_Slopes[i]["value"] < (-ps.S_th):
            TrueSlope_candles_count += 1
    is_slope_confirmed = (TrueSlope_candles_count == ps.M_Slope)

    # Check how many candles are above EMA in last K candles
    TrendCandles_count = 0
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, ps.K_Slope)
    EMA_key = IndicatorKey(symbol, timeframe, "EMA_FAST")
    EMAs = buffer_initializer.INDICATOR_BUFFER.last_n(EMA_key, ps.K_Slope)
    
    try:
        for j in range(len(candles)):
            if candles[j]["open"] < EMAs[j]["value"] and candles[j]["close"] < EMAs[j]["value"]:
                TrendCandles_count += 1
        is_Candles_with_trend_confirmed = (TrendCandles_count >= ps.N_Trend_Candle)

        if (is_slope_confirmed or ps.ignore_slope == 1) and is_Candles_with_trend_confirmed:
            return True
        else:
            return False
    except Exception as e:
        logger.error(f"Error in check_down_trend: {e}")
        return False
    
def check_trend():
    global Trend
    if check_up_trend():
        Trend = 1
    elif check_down_trend():
        Trend = -1
    else:
        Trend = 0

    # Append Trend to Indicator Buffer
    ind_key = IndicatorKey(symbol, timeframe, "Trend")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Trend})

def is_hammer_candle():
    global Hammer

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)

    if len(candles) < 1:
        ind_key = IndicatorKey(symbol, timeframe, "Hammer")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": 0})
        return False

    candle = candles[0]
    open_price = Decimal(str(candle["open"]))
    high_price = Decimal(str(candle["high"]))
    low_price = Decimal(str(candle["low"]))
    close_price = Decimal(str(candle["close"]))

    body = abs(close_price - open_price)
    upper_wick = high_price - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low_price
    candle_range = high_price - low_price

    # Standard hammer shape:
    # - small real body
    # - long lower shadow, typically at least 2x the body
    # - short upper shadow
    # - body sits near the top of the candle range
    if candle_range <= 0:
        Hammer = 0
    else:
        small_body = body <= (candle_range * Decimal("0.30"))
        long_lower_wick = lower_wick >= (body * Decimal("2")) if body > 0 else lower_wick > 0
        short_upper_wick = upper_wick <= body if body > 0 else upper_wick <= (candle_range * Decimal("0.10"))
        body_near_top = max(open_price, close_price) >= (low_price + (candle_range * Decimal("0.60")))

        Hammer = int(small_body and long_lower_wick and short_upper_wick and body_near_top)

    ind_key = IndicatorKey(symbol, timeframe, "Hammer")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Hammer})
    return bool(Hammer)

def is_shooting_star_candle():
    
    global Shooting_Star
    
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)

    if len(candles) < 1:
        ind_key = IndicatorKey(symbol, timeframe, "ShootingStar")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": 0})
        return False

    candle = candles[0]
    open_price = Decimal(str(candle["open"]))
    high_price = Decimal(str(candle["high"]))
    low_price = Decimal(str(candle["low"]))
    close_price = Decimal(str(candle["close"]))

    body = abs(close_price - open_price)
    upper_wick = high_price - max(open_price, close_price)
    lower_wick = min(open_price, close_price) - low_price
    candle_range = high_price - low_price

    # Standard shooting star shape (bearish opposite of hammer):
    # - small real body
    # - long upper shadow, typically at least 2x the body
    # - short lower shadow
    # - body sits near the bottom of the candle range
    if candle_range <= 0:
        Shooting_Star = 0
    else:
        small_body = body <= (candle_range * Decimal("0.30"))
        long_upper_wick = upper_wick >= (body * Decimal("2")) if body > 0 else upper_wick > 0
        short_lower_wick = lower_wick <= body if body > 0 else lower_wick <= (candle_range * Decimal("0.10"))
        body_near_bottom = min(open_price, close_price) <= (low_price + (candle_range * Decimal("0.40")))

        Shooting_Star = int(small_body and long_upper_wick and short_lower_wick and body_near_bottom)

    ind_key = IndicatorKey(symbol, timeframe, "ShootingStar")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Shooting_Star})
    return bool(Shooting_Star)

def is_3candle_strike_bullish():

    global Three_Candle_Strike_Bullish

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 4)

    if len(candles) < 4:
        Three_Candle_Strike_Bullish = 0
        ind_key = IndicatorKey(symbol, timeframe, "Bullish_3LineStrike")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Three_Candle_Strike_Bullish})
        return False

    candle_3 = candles[0]
    candle_2 = candles[1]
    candle_1 = candles[2]
    candle_0 = candles[3]

    close_3 = Decimal(str(candle_3["close"]))
    open_3 = Decimal(str(candle_3["open"]))
    close_2 = Decimal(str(candle_2["close"]))
    open_2 = Decimal(str(candle_2["open"]))
    close_1 = Decimal(str(candle_1["close"]))
    open_1 = Decimal(str(candle_1["open"]))
    close_0 = Decimal(str(candle_0["close"]))
    open_0 = Decimal(str(candle_0["open"]))

    Three_Candle_Strike_Bullish = int(
        close_3 < open_3
        and close_2 < open_2
        and close_1 < open_1
        and close_0 > open_1
    )

    ind_key = IndicatorKey(symbol, timeframe, "Bullish_3LineStrike")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Three_Candle_Strike_Bullish})
    return bool(Three_Candle_Strike_Bullish)

def get_HTF_candle_bios(timeframe: str):

    global HTF_candle_bios
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)
    HTF_candle = candles[0]
    if HTF_candle["close"] > HTF_candle["open"]:
        HTF_candle_bios = 1
    elif HTF_candle["close"] < HTF_candle["open"]:
        HTF_candle_bios = -1
    else:
        HTF_candle_bios = 0
    ind_key = IndicatorKey(symbol, timeframe, "HTF_candle_bios")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": HTF_candle_bios})
    
    return HTF_candle_bios

def is_3candle_strike_bearish():
    
    global Three_Candle_Strike_Bearish

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 4)

    if len(candles) < 4:
        Three_Candle_Strike_Bearish = 0
        ind_key = IndicatorKey(symbol, timeframe, "Bearish_3LineStrike")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Three_Candle_Strike_Bearish})
        return False

    candle_3 = candles[0]
    candle_2 = candles[1]
    candle_1 = candles[2]
    candle_0 = candles[3]

    close_3 = Decimal(str(candle_3["close"]))
    open_3 = Decimal(str(candle_3["open"]))
    close_2 = Decimal(str(candle_2["close"]))
    open_2 = Decimal(str(candle_2["open"]))
    close_1 = Decimal(str(candle_1["close"]))
    open_1 = Decimal(str(candle_1["open"]))
    close_0 = Decimal(str(candle_0["close"]))
    open_0 = Decimal(str(candle_0["open"]))

    Three_Candle_Strike_Bearish = int(
        close_3 > open_3
        and close_2 > open_2
        and close_1 > open_1
        and close_0 < open_1
    )

    ind_key = IndicatorKey(symbol, timeframe, "Bearish_3LineStrike")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": Three_Candle_Strike_Bearish})
    return bool(Three_Candle_Strike_Bearish)

def is_bullish_engulfing_candle():
    
    global bullish_engulfing

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 2)

    if len(candles) < 2:
        bullish_engulfing = 0
        ind_key = IndicatorKey(symbol, timeframe, "Bullish_eng")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": bullish_engulfing})
        return 

    current = candles[1]
    previous = candles[0]

    prev_open = Decimal(str(previous["open"]))
    prev_close = Decimal(str(previous["close"]))
    curr_open = Decimal(str(current["open"]))
    curr_close = Decimal(str(current["close"]))

    # Bullish engulfing definition:
    # - previous candle is bearish
    # - current candle is bullish
    # - current real body fully engulfs the previous real body
    if prev_close >= prev_open:
        bullish_engulfing = 0
    elif curr_close <= curr_open:
        bullish_engulfing = 0
    elif curr_open <= prev_close + Decimal("0.0001") and curr_close >= prev_open - Decimal("0.0001"):
        bullish_engulfing = 1
    else:
        bullish_engulfing = 0

    ind_key = IndicatorKey(symbol, timeframe, "Bullish_eng")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": bullish_engulfing})

def is_bearish_engulfing_candle():
    
    global bearish_engulfing

    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 2)

    if len(candles) < 2:
        bearish_engulfing = 0
        ind_key = IndicatorKey(symbol, timeframe, "Bearish_eng")
        buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": bearish_engulfing})
        return 

    current = candles[1]
    previous = candles[0]

    prev_open = Decimal(str(previous["open"]))
    prev_close = Decimal(str(previous["close"]))
    curr_open = Decimal(str(current["open"]))
    curr_close = Decimal(str(current["close"]))

    # Bearish engulfing definition:
    # - previous candle is bullish
    # - current candle is bearish
    # - current real body fully engulfs the previous real body
    if prev_close <= prev_open:
        bearish_engulfing = 0
    elif curr_close >= curr_open:
        bearish_engulfing = 0
    elif curr_open >= prev_close - Decimal("0.0001") and curr_close <= prev_open + Decimal("0.0001"):
        bearish_engulfing = 1
    else:
        bearish_engulfing = 0

    ind_key = IndicatorKey(symbol, timeframe, "Bearish_eng")
    buffers.INDICATOR_BUFFER.append(ind_key, {"event_time": close_time, "value": bearish_engulfing})

def set_details_for_pivots():

    lower_band_key = IndicatorKey(symbol, timeframe, "lower_band")
    lower_band_values = buffer_initializer.INDICATOR_BUFFER.last_n(lower_band_key, 200)
    upper_band_key = IndicatorKey(symbol, timeframe, "upper_band")
    upper_band_values = buffer_initializer.INDICATOR_BUFFER.last_n(upper_band_key, 200)

    pivot_registry: PivotBufferRegistry = get_pivot_registry()
    pb = pivot_registry.get(exchange, symbol, timeframe)

    lower_band_by_time = {
        item.get("event_time", item.get("eventtime")): Decimal(str(item["value"]))
        for item in lower_band_values
        if item.get("event_time", item.get("eventtime")) is not None and item.get("value") is not None
    }
    upper_band_by_time = {
        item.get("event_time", item.get("eventtime")): Decimal(str(item["value"]))
        for item in upper_band_values
        if item.get("event_time", item.get("eventtime")) is not None and item.get("value") is not None
    }
    
    last_peaks = list(pb.highs)
    for p in last_peaks:
        lower = lower_band_by_time.get(p.time)
        upper = upper_band_by_time.get(p.time)
        if lower is None or upper is None:
            p.close_inBands = False
            continue
        p.close_inBands = lower <= p.price <= upper

    last_low = list(pb.lows)
    for p in last_low:
        lower = lower_band_by_time.get(p.time)
        upper = upper_band_by_time.get(p.time)
        if lower is None or upper is None:
            p.close_inBands = False
            continue
        p.close_inBands = lower <= p.price <= upper

   
def update_pivot_buffer(timeframe: str):
    
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candles = buffer_initializer.CANDLE_BUFFER.last_n(key, 200)
    pivots = detect_market_structure_pivots(candles, zigzag_len=5)
    peaks, lows = prepare_pivot_list_for_pivot_buffer(pivots)
    
    pivot_registry: PivotBufferRegistry = get_pivot_registry()
    pb = pivot_registry.get(exchange, symbol, timeframe)
    pb.highs.clear()
    pb.lows.clear()

    for i, peak in reversed(list(enumerate(peaks))):
        pb.add_peak(peak)

    for i, low in reversed(list(enumerate(lows))):
        pb.add_low(low)
    
    set_details_for_pivots()

# ---------- PivotList snapshot helpers ----------

def _gather_pivot_list_rows(tf: str) -> List[tuple]:
    """
    Build the full rows for pivot_list at this event_time across all EXPECTED_SYMBOLS.
    Returns list of tuples:
      (event_time, symbol, timeframe, pivot_type, pivot_time, pivot_open_time, price, hit, hit_distance, type2, close_inBands)
    """
    rows: List[tuple] = []

    pivot_registry: PivotBufferRegistry = get_pivot_registry()
    pb = pivot_registry.get(exchange, symbol, tf)

    # Iterate highs
    try:
        for p in pb.iter_peaks_newest_first():
            pivot_time = getattr(p, "close_time", None) or getattr(p, "time", None)
            pivot_open_time = getattr(p, "open_time", None)
            price = getattr(p, "level", None)
            if price is None:
                price = getattr(p, "price", None)
            hit = bool(getattr(p, "hit", getattr(p, "is_hit", False)))
            type2 = getattr(p, "type2", None)
            close_inBands = getattr(p, "close_inBands", None)

            rows.append((
                close_time,            # event_time
                symbol,                # symbol
                tf,                    # timeframe
                "HIGH",                # pivot_type
                pivot_time,            # pivot_time (close_time of that candle)
                pivot_open_time,       # pivot_open_time (if available)
                price,                 # price
                hit,                   # hit
                getattr(p, "hit_distance", None), #hit_distance
                type2,                 # type2 (e.g. "HH", "LL", "LH", "HL")
                close_inBands,         # close_inBands (e.g. for Keltner Bands)
            ))
    except Exception:
        pass

    # Iterate lows
    try:
        for p in pb.iter_lows_newest_first():
            pivot_time = getattr(p, "close_time", None) or getattr(p, "time", None)
            pivot_open_time = getattr(p, "open_time", None)
            price = getattr(p, "level", None)
            if price is None:
                price = getattr(p, "price", None)
            hit = bool(getattr(p, "hit", getattr(p, "is_hit", False)))
            type2 = getattr(p, "type2", None)
            close_inBands = getattr(p, "close_inBands", None)

            rows.append((
                close_time,
                symbol,
                tf,                    # timeframe
                "LOW",
                pivot_time,
                pivot_open_time,
                price,
                hit,
                getattr(p, "hit_distance", None), #hit_distance
                type2,                 # type2 (e.g. "HH", "LL", "LH", "HL")
                close_inBands,         # close_inBands (e.g. for Keltner Bands)
            ))
    except Exception:
        pass

    return rows

def _async_insert_pivot_list(rows: List[tuple]) -> None:

    if not rows:
        return

    def _worker(batch: List[tuple]) -> None:
        try:
            conn = get_pg_conn()
            sql = """
                INSERT INTO pivot_list (
                    event_time, symbol, timeframe, pivot_type, pivot_time, pivot_open_time, price, hit, hit_distance, type2, close_inBands
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            with conn.cursor() as cur:
                cur.executemany(sql, batch)
            conn.commit()
            conn.close()
        except Exception as e:
            # Soft-fail: keep processing without raising
            logger.error(f"[pivot_list insert] error: {e}")

    t = threading.Thread(target=_worker, args=(rows,), daemon=True)
    t.start()


def is_valid_signal_detected_3LineStrike() -> tuple[bool, str, Decimal, Decimal]:
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 4)

    if len(candle) < 4:
        return False, None, None, None

    c1 = candle[0]
    c2 = candle[1]
    c3 = candle[2]
    c4 = candle[3]  # Latest closed candle

    o1 = Decimal(str(c1["open"]))
    h1 = Decimal(str(c1["high"]))
    l1 = Decimal(str(c1["low"]))
    c1_close = Decimal(str(c1["close"]))

    o2 = Decimal(str(c2["open"]))
    h2 = Decimal(str(c2["high"]))
    l2 = Decimal(str(c2["low"]))
    c2_close = Decimal(str(c2["close"]))

    o3 = Decimal(str(c3["open"]))
    h3 = Decimal(str(c3["high"]))
    l3 = Decimal(str(c3["low"]))
    c3_close = Decimal(str(c3["close"]))

    o4 = Decimal(str(c4["open"]))
    h4 = Decimal(str(c4["high"]))
    l4 = Decimal(str(c4["low"]))
    c4_close = Decimal(str(c4["close"]))

    # Skip if last candle range is too large (>= 2 ATR)
    if (h4 - l4) >= ATR * Decimal("2"):
        return False, None, None, None

    # --- Short setup ---
    # Last candle red, previous 3 candles green, and last candle body >= 40% of
    # the total range covered by those 3 previous candles.
    prev_3_green = (c1_close > o1) and (c2_close > o2) and (c3_close > o3)
    last_red = c4_close < o4
    red_body = o4 - c4_close
    green_range = max(h1, h2, h3) - min(l1, l2, l3)
    size_ok_for_short = red_body >= (green_range * Decimal("0.40")) if green_range > 0 else False

    if (
        prev_3_green
        and last_red
        and size_ok_for_short
        and c4_close < EMA_FAST
        and EMA_FAST < EMA_SLOW
    ):
        sl_price = c4_close + ATR
        distance = sl_price - c4_close
        target_price = c4_close - (distance * Decimal("2.0"))  # 1:1.5 RR
        return True, "SELL", target_price, sl_price

    # --- Long setup (opposite logic) ---
    prev_3_red = (c1_close < o1) and (c2_close < o2) and (c3_close < o3)
    last_green = c4_close > o4
    green_body = c4_close - o4
    red_range = max(h1, h2, h3) - min(l1, l2, l3)
    size_ok_for_long = green_body >= (red_range * Decimal("0.40")) if red_range > 0 else False

    if (
        prev_3_red
        and last_green
        and size_ok_for_long
        and c4_close > EMA_FAST
        and EMA_FAST > EMA_SLOW
    ):
        sl_price = c4_close - ATR
        distance = c4_close - sl_price
        target_price = c4_close + (distance * Decimal("2.0"))  # 1:1.5 RR
        return True, "BUY", target_price, sl_price

    return False, None, None, None

def is_valid_signal_HTF_Range() -> tuple[bool, str, Decimal, Decimal]:
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 2)

    runtime = _load_htf_runtime()
    if not runtime:
        return

    htf_key = Keys(exchange=exchange, symbol=symbol, timeframe=runtime["htf_tf"])
    HTF_Candle = buffer_initializer.CANDLE_BUFFER.last_n(htf_key, 1)

    HTF_ind_key = IndicatorKey(symbol, runtime["htf_tf"], "ATR")
    HTF_ATR_values = buffer_initializer.INDICATOR_BUFFER.last_n(HTF_ind_key, 1)
    HTF_ATR = Decimal(str(HTF_ATR_values[0]["value"])) if HTF_ATR_values else None

    if len(HTF_Candle) < 1:
        return False, None, None, None

    current_open = Decimal(str(candle[1]["open"]))
    current_high = Decimal(str(candle[1]["high"]))
    current_low = Decimal(str(candle[1]["low"]))
    current_close = Decimal(str(candle[1]["close"]))

    HTF_open = Decimal(str(HTF_Candle[0]["open"]))
    HTF_high = Decimal(str(HTF_Candle[0]["high"]))
    HTF_low = Decimal(str(HTF_Candle[0]["low"]))
    HTF_close = Decimal(str(HTF_Candle[0]["close"]))

    # Skip if last candle range is too large (>= 2 ATR)
    # or if close is too close to open (potentially weak signal)
    '''
    if (HTF_high - HTF_low >= ATR * Decimal("2")
        or abs(HTF_close - HTF_open) < ATR * Decimal("0.5")):
        return False, None, None, None
    '''

    #logger.info(f"HTF Range Check: HTF_open={HTF_open}, HTF_high={HTF_high}, HTF_low={HTF_low}, HTF_close={HTF_close}, current_open={current_open}, current_close={current_close}")
    #logger.info(f"LTF Range Check: LTF_open={current_open}, LTF_high={current_high}, LTF_low={current_low}, LTF_close={current_close}, HTF_open={HTF_open}, HTF_high={HTF_high}, HTF_low={HTF_low}, HTF_close={HTF_close}")
    # --- Long setup ---
    if (
        current_open < current_close # the LTF candle is green
        and current_close > HTF_high # the LTF candle is green
        and HTF_open < HTF_close # the HTF candle is green
    ):
        sl_price = Decimal(str(candle[0]["low"]))
        sl_distance = Decimal(str(candle[1]["close"])) - sl_price
        target_price = Decimal(str(candle[0]["close"])) + (sl_distance * Decimal("1.5"))
        return True, "BUY", target_price, sl_price

    # --- Short setup ---
    if (
        current_open > current_close # the LTF candle is red
        and current_close < HTF_low # the LTF candle is red
        and HTF_open > HTF_close # the HTF candle is red
    ):
        sl_price = Decimal(str(candle[0]["high"]))
        sl_distance = sl_price - Decimal(str(candle[1]["close"]))
        target_price = Decimal(str(candle[0]["close"])) - (sl_distance * Decimal("1.5"))
        return True, "SELL", target_price, sl_price


    return False, None, None, None

def is_valid_signal_detected() -> tuple[bool, str, Decimal, Decimal]:
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 1)
    close_current = Decimal(str(candle[0]["close"]))
    open_current = Decimal(str(candle[0]["open"]))
    low_current = Decimal(str(candle[0]["low"]))
    high_current = Decimal(str(candle[0]["high"]))

    ltf_high, ltf_low, htf_high, htf_low = recent_swings_touching_bands() 

    # checks for buy signal
    if (close_current > open_current 
        and close_current > EMA_FAST
        and EMA_FAST > EMA_SLOW
        and (bullish_engulfing or Three_Candle_Strike_Bullish or Hammer)
        and HTF_candle_bios == 1
        and RSI < Decimal("70") and RSI > Decimal("50")
        and MACD_LINE > MACD_SIGNAL and MACD_LINE > 0 and MACD_LINE - MACD_SIGNAL > Decimal("0.00005")
        and MACD_HISTOGRAM_EXPANDING == 1):
        
        sl_price = close_current - max(Decimal(close_current - low_current), ATR)
        distance = close_current - sl_price
        
        # Determine nearest pivot high (LTF or HTF)
        nearest_pivot_high = None
        if ltf_high is not None and htf_high is not None:
            nearest_pivot_high = min(ltf_high, htf_high)
        elif ltf_high is not None:
            nearest_pivot_high = ltf_high
        elif htf_high is not None:
            nearest_pivot_high = htf_high
        
        # Calculate initial target with configured RR_ratio
        suggested_target = Decimal(close_current + (distance * Decimal(str(ps.RR_ratio))))
        return True, "BUY", suggested_target, sl_price
    
        '''
        # If no pivot to constrain against, use initial target
        if nearest_pivot_high is None:
            return True, "BUY", suggested_target, sl_price
        
        # Calculate 85% boundary: entry + 85% of (pivot - entry)
        max_target_price = close_current + ((nearest_pivot_high - close_current) * Decimal("0.85"))
        
        # If initial target is already safe (below 85% boundary), use it
        if suggested_target <= max_target_price:
            return True, "BUY", suggested_target, sl_price
        
        # Otherwise, loop to reduce RR_ratio until target fits boundary or RR < 1.0
        current_rr = Decimal(str(ps.RR_ratio))
        
        while current_rr >= Decimal("1.0"):
            adjusted_target = Decimal(close_current + (distance * current_rr))
            
            if adjusted_target <= max_target_price:
                return True, "BUY", adjusted_target, sl_price
            
            current_rr -= Decimal("0.1")
        

        # No safe target found with RR >= 1.0: skip signal
        return False, None, None, None
        '''
    #=========================================

    # checks for sell signal
    if (close_current < open_current
        and close_current < EMA_FAST
        and EMA_FAST < EMA_SLOW
        and (bearish_engulfing or Three_Candle_Strike_Bearish or Shooting_Star)
        and HTF_candle_bios == -1
        and RSI > Decimal("30") and RSI < Decimal("50")
        and MACD_LINE < MACD_SIGNAL and MACD_LINE < 0 and MACD_SIGNAL - MACD_LINE > Decimal("0.00005")
        and MACD_HISTOGRAM_EXPANDING == 1):

        sl_price = close_current + max(Decimal(high_current - close_current), ATR)
        distance = sl_price - close_current
        
        # Determine nearest pivot low (LTF or HTF)
        nearest_pivot_low = None
        if ltf_low is not None and htf_low is not None:
            nearest_pivot_low = max(ltf_low, htf_low)
        elif ltf_low is not None:
            nearest_pivot_low = ltf_low
        elif htf_low is not None:
            nearest_pivot_low = htf_low
        
        # Calculate initial target with configured RR_ratio
        suggested_target = Decimal(close_current - (distance * Decimal(str(ps.RR_ratio))))
        return True, "SELL", suggested_target, sl_price
    
        '''
        # If no pivot to constrain against, use initial target
        if nearest_pivot_low is None:
            return True, "SELL", suggested_target, sl_price
        
        # Calculate 85% boundary: entry - 85% of (entry - pivot)
        max_target_price = close_current - ((close_current - nearest_pivot_low) * Decimal("0.85"))
        
        # If initial target is already safe (above 85% boundary), use it
        if suggested_target >= max_target_price:
            return True, "SELL", suggested_target, sl_price
        
        # Otherwise, loop to reduce RR_ratio until target fits boundary or RR < 1.0
        current_rr = Decimal(str(ps.RR_ratio))
        
        while current_rr >= Decimal("1.0"):
            adjusted_target = Decimal(close_current - (distance * current_rr))
            
            if adjusted_target >= max_target_price:
                return True, "SELL", adjusted_target, sl_price
            
            current_rr -= Decimal("0.1")
        
        # No safe target found with RR >= 1.0: skip signal

        return False, None, None, None
        '''
    #=========================================
   
    return False, None, None, None

def recent_swings_touching_bands() -> tuple[
    Optional[Decimal],  # LTF swing high (not hit)
    Optional[Decimal],  # LTF swing low  (not hit)
    Optional[Decimal],  # HTF swing high (not hit, verified against LTF candles)
    Optional[Decimal],  # HTF swing low  (not hit, verified against LTF candles)
]:
    """
    Returns the most recent un-hit swing high/low prices for both the current
    (lower) timeframe and the higher timeframe, sourced from PivotBufferRegistry.

    For HTF pivots the stored is_hit flag may be stale (it is only refreshed when
    the next HTF candle closes). Instead, each HTF pivot is re-checked in real-time
    by scanning all LTF candles that exist AFTER the pivot was formed:
      - A HTF swing high is considered hit if any LTF candle's high >= pivot price.
      - A HTF swing low  is considered hit if any LTF candle's low  <= pivot price.
    Only HTF pivots formed within the LTF candle history window are checked this way;
    older ones are evaluated using their stored is_hit flag.
    """
    pivot_registry: PivotBufferRegistry = get_pivot_registry()

    # ---- LTF pivots — use stored is_hit directly ----
    ltf_pb = pivot_registry.get(exchange, symbol, timeframe)

    ltf_high: Optional[Decimal] = None
    for p in ltf_pb.iter_peaks_newest_first():
        if not p.is_hit and p.price is not None:
            ltf_high = Decimal(str(p.price))
            break

    ltf_low: Optional[Decimal] = None
    for p in ltf_pb.iter_lows_newest_first():
        if not p.is_hit and p.price is not None:
            ltf_low = Decimal(str(p.price))
            break

    # ---- HTF pivots — re-verify hit status against LTF candles ----
    htf_high: Optional[Decimal] = None
    htf_low: Optional[Decimal] = None

    runtime = _load_htf_runtime()
    if runtime:
        htf_tf = runtime.get("htf_tf")
        if htf_tf:
            htf_pb = pivot_registry.get(exchange, symbol, htf_tf)

            # Load all available LTF candles once for hit checking.
            ltf_key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
            ltf_candle_count = buffer_initializer.CANDLE_BUFFER.get_len(ltf_key)
            ltf_candles = buffer_initializer.CANDLE_BUFFER.last_n(ltf_key, ltf_candle_count)

            # Determine the open_time of the oldest LTF candle in the buffer so we
            # know which HTF pivots fall within our observable window.
            ltf_oldest_time: Optional[datetime] = None
            if ltf_candles:
                raw = ltf_candles[0].get("open_time")
                if isinstance(raw, datetime):
                    ltf_oldest_time = _normalize_dt(raw)

            def _htf_pivot_is_hit_by_ltf(pivot_price: Decimal, check_high: bool) -> bool:
                """
                Scan LTF candles that closed AFTER this pivot was formed and check
                whether price has been reached.
                """
                pivot_time_raw = None  # not available in this closure; scan all LTF
                for c in ltf_candles:
                    if check_high:
                        if Decimal(str(c["high"])) >= pivot_price:
                            return True
                    else:
                        if Decimal(str(c["low"])) <= pivot_price:
                            return True
                return False

            # Find most recent HTF swing high not hit by any LTF candle.
            for p in htf_pb.iter_peaks_newest_first():
                if p.price is None:
                    continue
                pivot_price = Decimal(str(p.price))

                # If the pivot pre-dates our LTF window, fall back to stored flag.
                pivot_time_raw = getattr(p, "time", None) or getattr(p, "close_time", None)
                pivot_within_window = False
                if ltf_oldest_time is not None and isinstance(pivot_time_raw, datetime):
                    pivot_within_window = _normalize_dt(pivot_time_raw) >= ltf_oldest_time

                if pivot_within_window:
                    hit = _htf_pivot_is_hit_by_ltf(pivot_price, check_high=True)
                else:
                    hit = p.is_hit

                if not hit:
                    htf_high = pivot_price
                    break

            # Find most recent HTF swing low not hit by any LTF candle.
            for p in htf_pb.iter_lows_newest_first():
                if p.price is None:
                    continue
                pivot_price = Decimal(str(p.price))

                pivot_time_raw = getattr(p, "time", None) or getattr(p, "close_time", None)
                pivot_within_window = False
                if ltf_oldest_time is not None and isinstance(pivot_time_raw, datetime):
                    pivot_within_window = _normalize_dt(pivot_time_raw) >= ltf_oldest_time

                if pivot_within_window:
                    hit = _htf_pivot_is_hit_by_ltf(pivot_price, check_high=False)
                else:
                    hit = p.is_hit

                if not hit:
                    htf_low = pivot_price
                    break

    return ltf_high, ltf_low, htf_high, htf_low

def is_valid_signal_detected_3() -> tuple[bool, str, Decimal, Decimal]:

    signals = open_sig_registry._signals_by_symbol.get(symbol)
    if signals:
        return False, None, None, None
    
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 4)
    close_current = Decimal(str(candle[3]["close"]))

    #--------------------------------------------------------

    # checks for buy signal
    if (EMA_FAST > EMA_SLOW
        and Decimal(str(candle[3]["close"])) > Decimal(str(candle[3]["open"]))
        and Decimal(str(candle[3]["close"])) > EMA_FAST
        and Decimal(str(candle[2]["close"])) > EMA_FAST
        and Decimal(str(candle[1]["close"])) > EMA_FAST
        and Decimal(str(candle[0]["close"])) > EMA_FAST
        #and ADX >= Decimal("25") #and PLUS_DI > MINUS_DI
        #and Volume_AVG < Decimal(str(candle[3]["volume"]))
        ):

        sl_price = min(Decimal(str(candle[3]["close"])) - ATR, Decimal(str(candle[3]["low"])))
        target_price = close_current + ((close_current - sl_price) * Decimal(ps.RR_ratio))

        return True, "BUY", target_price, Decimal(1) #sl_price
    #=========================================

    # checks for sell signal
    if (EMA_FAST < EMA_SLOW
        and Decimal(str(candle[3]["close"])) < Decimal(str(candle[3]["open"]))
        and Decimal(str(candle[3]["close"])) < EMA_FAST
        and Decimal(str(candle[2]["close"])) < EMA_FAST
        and Decimal(str(candle[1]["close"])) < EMA_FAST
        and Decimal(str(candle[0]["close"])) < EMA_FAST
        #and ADX >= Decimal("25") #and PLUS_DI > MINUS_DI
        #and Volume_AVG < Decimal(str(candle[3]["volume"]))
        ):
        
        sl_price = max(Decimal(str(candle[3]["close"])) + ATR, Decimal(str(candle[3]["high"])))
        target_price = close_current - ((sl_price - close_current) * Decimal(ps.RR_ratio))

        return True, "SELL", target_price, Decimal(2) #sl_price
    #=========================================
    #   
    '''
    if bullish_engulfing == 1 and Trend == 1:
        return True, "BUY", (close + ((close - open + ATR) * Decimal(1.5))), open - ATR
    if bearish_engulfing == 1 and Trend == -1:
        return True, "SELL", (close - ((open - close + ATR) * Decimal(1.5))), open + ATR
    '''

    '''
    if open > close:
        return True, "BUY", (close + (ATR * Decimal(2))), open - ATR * Decimal(2)
    if open < close:
        return True, "SELL", (close - (ATR * Decimal(2))), open + ATR * Decimal(2)
    '''   
        
    return False, None, None, None

def is_valid_signal_detected_2() -> tuple[bool, str, Decimal, Decimal]:
    key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    candle = buffer_initializer.CANDLE_BUFFER.last_n(key, 3)
    close_current = Decimal(str(candle[2]["close"]))
    open_current = Decimal(str(candle[2]["open"]))

    close_prev = Decimal(str(candle[1]["close"]))
    open_prev = Decimal(str(candle[1]["open"]))
  
    close_3 = Decimal(str(candle[0]["close"]))
    open_3 = Decimal(str(candle[0]["open"]))

    # candles body checks
    body_prev_pass = False
    body_3_pass = False
    engulfing_body_pass = False

    body_prev = abs(close_prev - open_prev)
    body_3 = abs(close_3 - open_3)
    if body_prev >= (ATR * Decimal("0.2")):
        body_prev_pass = True
    if body_3 >= (ATR * Decimal("0.2")):
        body_3_pass = True

    engulfing_body = abs(close_current - open_current)
    #if (engulfing_body >= Decimal(1.2) * max(body_prev, body_3)) and (engulfing_body <= ATR * Decimal("1.5")):
    if (engulfing_body <= ATR * Decimal("1.5")):
        engulfing_body_pass = True
    #--------------------------------------------------------

    # checks for buy signal
    if (bullish_engulfing == 1 
        and Trend == 1 
        and close_3 < open_3 
        #and RSI >= Decimal("50") 
        #and ADX > Decimal("25") and PLUS_DI > MINUS_DI
        #and body_prev_pass 
        #and body_3_pass 
        and engulfing_body_pass):

        atr_val = ATR * Decimal(ps.SL_ATR_K)
        structure_sl = min(Decimal(str(candle[2]["low"])), Decimal(str(candle[1]["low"])))
        sl_price = min(structure_sl, close_current - atr_val)
        #sl_price = structure_sl
        target_price = close_current + ((close_current - sl_price) * Decimal(ps.RR_ratio))

        return True, "BUY", target_price, sl_price
    #=========================================

    # checks for sell signal
    if (bearish_engulfing == 1 
        and Trend == -1 
        and close_3 > open_3 
        #and RSI <= Decimal("50") 
        #and ADX > Decimal("25") and MINUS_DI > PLUS_DI
        #and body_prev_pass 
        #and body_3_pass 
        and engulfing_body_pass):
        
        atr_val = ATR * Decimal(ps.SL_ATR_K)
        structure_sl = max(Decimal(str(candle[2]["high"])), Decimal(str(candle[1]["high"])))
        sl_price = max(structure_sl, close_current + atr_val)
        #sl_price = structure_sl
        target_price = close_current - ((sl_price - close_current) * Decimal(ps.RR_ratio))

        return True, "SELL", target_price, sl_price
    #=========================================
    #   
    '''
    if bullish_engulfing == 1 and Trend == 1:
        return True, "BUY", (close + ((close - open + ATR) * Decimal(1.5))), open - ATR
    if bearish_engulfing == 1 and Trend == -1:
        return True, "SELL", (close - ((open - close + ATR) * Decimal(1.5))), open + ATR
    '''

    '''
    if open > close:
        return True, "BUY", (close + (ATR * Decimal(2))), open - ATR * Decimal(2)
    if open < close:
        return True, "SELL", (close - (ATR * Decimal(2))), open + ATR * Decimal(2)
    '''   
        
    return False, None, None, None


def record_strategy_modules_history(timeframe: str):
    #global strategy_modules_history_rows

    strategy_modules_history_rows = [
        (close_time, symbol, timeframe, "ATR", ATR),
        (close_time, symbol, timeframe, "EMA_FAST", EMA_FAST),
        (close_time, symbol, timeframe, "EMA_SLOW", EMA_SLOW),
        (close_time, symbol, timeframe, "inBands", inBands),
        (close_time, symbol, timeframe, "EMA_Slope", EMA_Slope),
        (close_time, symbol, timeframe, "upper_band", upper_band),
        (close_time, symbol, timeframe, "lower_band", lower_band),
        (close_time, symbol, timeframe, "Trend", Trend),
        (close_time, symbol, timeframe, "Bullish_eng", bullish_engulfing),
        (close_time, symbol, timeframe, "Bearish_eng", bearish_engulfing),
        (close_time, symbol, timeframe, "RSI", RSI),
        (close_time, symbol, timeframe, "ADX", ADX),
        (close_time, symbol, timeframe, "CE_LONG", CE_LONG),
        (close_time, symbol, timeframe, "CE_SHORT", CE_SHORT),
        (close_time, symbol, timeframe, "Volume_AVG", Volume_AVG),
        (close_time, symbol, timeframe, "Hammer", Hammer),
        (close_time, symbol, timeframe, "ShootingStar", Shooting_Star),
        (close_time, symbol, timeframe, "Bullish_3LineStrike", Three_Candle_Strike_Bullish),
        (close_time, symbol, timeframe, "Bearish_3LineStrike", Three_Candle_Strike_Bearish),
        (close_time, symbol, timeframe, "MACD_LINE", MACD_LINE),
        (close_time, symbol, timeframe, "MACD_SIGNAL", MACD_SIGNAL),
        (close_time, symbol, timeframe, "MACD_HISTOGRAM", MACD_HISTOGRAM),
        (close_time, symbol, timeframe, "MACD_HISTOGRAM_EXPANDING", MACD_HISTOGRAM_EXPANDING)
    ]
    #strategy_modules_history_rows.extend(rows_to_add)

    insert_strategy_modules_history(rows=strategy_modules_history_rows)
    
    
    try:
        rows = _gather_pivot_list_rows(timeframe)
        _async_insert_pivot_list(rows)
    except Exception as e:
        logger.error(f"[pivot_list snapshot] error: {e}")
    


def record_strategy_modules_history_HTF(timeframe: str):
    #global strategy_modules_history_rows

    strategy_modules_history_rows = [
        #(close_time, symbol, timeframe, "ATR", ATR),
        #(close_time, symbol, timeframe, "EMA_FAST", EMA_FAST),
        (close_time, symbol, timeframe, "EMA_SLOW", EMA_SLOW),
        #(close_time, symbol, timeframe, "inBands", inBands),
        #(close_time, symbol, timeframe, "EMA_Slope", EMA_Slope),
        #(close_time, symbol, timeframe, "upper_band", upper_band),
        #(close_time, symbol, timeframe, "lower_band", lower_band),
        #(close_time, symbol, timeframe, "Trend", Trend),
        #(close_time, symbol, timeframe, "Bullish_eng", bullish_engulfing),
        #(close_time, symbol, timeframe, "Bearish_eng", bearish_engulfing),
        #(close_time, symbol, timeframe, "RSI", RSI),
        (close_time, symbol, timeframe, "ADX", ADX),
        #(close_time, symbol, timeframe, "CE_LONG", CE_LONG),
        #(close_time, symbol, timeframe, "CE_SHORT", CE_SHORT),
        #(close_time, symbol, timeframe, "Volume_AVG", Volume_AVG),
        (close_time, symbol, timeframe, "HTF_candle_bio", HTF_candle_bios),
    ]
    #strategy_modules_history_rows.extend(rows_to_add)

    insert_strategy_modules_history(rows=strategy_modules_history_rows)
    
    
    try:
        rows = _gather_pivot_list_rows(timeframe)
        _async_insert_pivot_list(rows)
    except Exception as e:
        logger.error(f"[pivot_list snapshot] error: {e}")


#=========================
def debug_pivots(candles, zigzag_len=5):
    """Debug: shows EXACTLY what we're finding vs what's missing"""
    print(f"DEBUG: Testing {len(candles)} candles, zigzag_len={zigzag_len}")
    
    # Show last 10 candles context
    print("\n=== LAST 10 CANDLES ===")
    for i in range(-10, 0):
        c = candles[i]
        print(f"idx{i}: H={c['high']:.5f} L={c['low']:.5f} "
              f"[{pm.format_time_simple(str(c['open_time']))}]")
    
    pivots = detect_market_structure_pivots(candles, zigzag_len, True)
    print_pivots(pivots)
    
    # Show ALL raw pivot highs/lows found
    print("\n=== RAW PIVOT HIGHS FOUND ===")
    # ... (add raw pivot finder code)
    
    return pivots


def manage_HTF():
    runtime = _load_htf_runtime()
    if not runtime:
        return

    if timeframe != runtime["ltf_tf"]:
        return

    ltf_key = Keys(exchange=exchange, symbol=symbol, timeframe=timeframe)
    latest = buffer_initializer.CANDLE_BUFFER.last_n(ltf_key, 1)
    if not latest:
        return

    new_ltf = latest[-1]
    new_close_raw = new_ltf.get("close_time")
    if not isinstance(new_close_raw, datetime):
        return

    new_close = _normalize_dt(new_close_raw)
    ltf_sec = runtime["ltf_sec"]
    htf_sec = runtime["htf_sec"]
    ratio = runtime["ratio"]

    # Build HTF candle only on Forex-aligned HTF boundary (relative to 21:00 UTC market start).
    # Example for 4h: closes at 01:00, 05:00, 09:00, 13:00, 17:00, 21:00 UTC.
    seconds_since_market_start = _get_seconds_since_forex_market_start(new_close)
    if seconds_since_market_start % htf_sec != 0:
        return

    # HTF candle opens htf_sec seconds before close (measured from market start boundary)
    htf_open = new_close - timedelta(seconds=htf_sec)
    ltf_window = buffer_initializer.CANDLE_BUFFER.last_n(ltf_key, ratio)
    if len(ltf_window) < ratio:
        return

    first_open_raw = ltf_window[0].get("open_time")
    last_close_raw = ltf_window[-1].get("close_time")
    if not isinstance(first_open_raw, datetime) or not isinstance(last_close_raw, datetime):
        return

    first_open = _normalize_dt(first_open_raw)
    last_close = _normalize_dt(last_close_raw)
    if first_open != htf_open or last_close != new_close:
        return

    expected_open = htf_open
    for candle in ltf_window:
        c_open_raw = candle.get("open_time")
        c_close_raw = candle.get("close_time")
        if not isinstance(c_open_raw, datetime) or not isinstance(c_close_raw, datetime):
            return

        c_open = _normalize_dt(c_open_raw)
        c_close = _normalize_dt(c_close_raw)

        if c_open != expected_open:
            return
        if int((c_close - c_open).total_seconds()) != ltf_sec:
            return

        expected_open = c_close

    if expected_open != new_close:
        return

    htf_key = Keys(exchange=exchange, symbol=symbol, timeframe=runtime["htf_tf"])
    last_htf = buffer_initializer.CANDLE_BUFFER.last_n(htf_key, 1)
    if last_htf:
        existing_close = last_htf[-1].get("close_time")
        if isinstance(existing_close, datetime) and _normalize_dt(existing_close) == new_close:
            return

    htf_candle = {
        "exchange": exchange,
        "symbol": symbol,
        "timeframe": runtime["htf_tf"],
        "open_time": htf_open,
        "close_time": new_close,
        "open": float(ltf_window[0]["open"]),
        "high": float(max(Decimal(str(c["high"])) for c in ltf_window)),
        "low": float(min(Decimal(str(c["low"])) for c in ltf_window)),
        "close": float(ltf_window[-1]["close"]),
        "volume": float(sum(Decimal(str(c.get("volume", 0))) for c in ltf_window)),
    }

    for field in ("base_currency", "quote_currency"):
        if field in new_ltf:
            htf_candle[field] = new_ltf[field]

    buffers.CANDLE_BUFFER.append(htf_key, htf_candle)

    global Candle_Count_After_HTF_Reset
    Candle_Count_After_HTF_Reset = 0
    


    #Calculate Indicators for HTF candle immediately after creation
    #calculate_ATR(runtime["htf_tf"])

    '''
    calculate_ADX(runtime["htf_tf"])
    update_pivot_buffer(runtime["htf_tf"])
    get_HTF_candle_bios(runtime["htf_tf"])
    '''
    
    # Records values and history
    record_strategy_modules_history_HTF(runtime["htf_tf"])
    #=========================================================================================

    logger.info(
        "HTF candle created %s %s %s -> %s",
        exchange,
        symbol,
        runtime["htf_tf"],
        pm.format_time_simple(str(new_close)),
    )

