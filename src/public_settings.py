
from decimal import Decimal
from db_general import get_public_settings

symbol = None

def load_settings_from_db():
    settings = get_public_settings()
    global ATR_LEN, EMA_FAST_LEN, EMA_SLOW_LEN, K_mult, RR_ratio, max_bars_since_trade, M_Slope, K_Slope, S_th, N_Trend_Candle, ExecuteID, DEFAULT_ORDER_UNITS, ignore_slope, ignore_session_ckeck, Candle_Limit_offset, New_Candle_Follow_Price_pct, min_pip_for_TSL, ignore_follow_price, Start_Date_Time, End_Date_Time, SL_ATR_K
    ATR_LEN = int(settings.get('ATR_LEN', 14))
    EMA_FAST_LEN = int(settings.get('EMA_FAST_LEN', 20))
    EMA_SLOW_LEN = int(settings.get('EMA_SLOW_LEN', 50))
    K_mult = Decimal(settings.get('K_mult', 2.0))
    RR_ratio = Decimal(settings.get('RR_ratio', 2.0))
    max_bars_since_trade = int(settings.get('max_bars_since_trade', 5))
    M_Slope = int(settings.get('M_Slope', 5))
    K_Slope = int(settings.get('K_Slope', 20))
    S_th = Decimal(settings.get('S_th', 0.04))
    N_Trend_Candle = int(settings.get('N_Trend_Candle', 15))
    N_Trend_Candle = int(settings.get('N_Trend_Candle', 15))
    DEFAULT_ORDER_UNITS = int(settings.get('DEFAULT_ORDER_UNITS', 10000))
    ignore_slope = int(settings.get('ignore_slope', 1))

    Start_Date_Time = settings.get('Start_Date_Time')
    End_Date_Time = settings.get('End_Date_Time')

    ignore_session_ckeck = int(settings.get('ignore_session_ckeck', 0))

    ExecuteID = int(settings.get('ExecuteID', 0))

    Candle_Limit_offset = int(40)
    New_Candle_Follow_Price_pct = Decimal(settings.get('New_Candle_Follow_Price_pct', 0.3))
    min_pip_for_TSL = Decimal(settings.get('min_pip_for_TSL', 6.0))
    ignore_follow_price = int(settings.get('ignore_follow_price', 0))
    SL_ATR_K = Decimal(settings.get('SL_ATR_K', 1.5))
