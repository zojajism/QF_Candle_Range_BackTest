import datetime
from decimal import Decimal
import math
from os import truncate

from dateutil import parser
import public_settings as ps



def format_time_simple(iso_time_str):
    """
    Convert ISO8601 string like '2026-02-19T01:04:00.000+00:00' to '2026-02-19 01:04'.
    """
    dt = parser.parse(iso_time_str)
    return dt.strftime('%Y-%m-%d %H:%M')

def _to_oanda_instrument(symbol: str) -> str:
    """
    Convert internal symbol like 'EUR/USD' to OANDA instrument 'EUR_USD'.
    """
    return symbol.replace("/", "_")

fmt = lambda v, n: "N/A" if v is None else truncate(v, n)

def truncate(value: float, decimals: int) -> float:
    factor = 10 ** decimals
    return math.trunc(value * factor) / factor

def _pip_size(symbol: str) -> Decimal:
    s = symbol.upper()
    if "JPY" in s or "DXY" in s:
        return Decimal("0.01")
    return Decimal("0.0001")


TRADING_HOURS = {
    0: [("00:00", "23:59")],  # Monday
    1: [("00:00", "23:59")],  # Tuesday
    2: [("00:00", "23:59")],  # Wednesday
    3: [("00:00", "23:59")],  # Thursday
    4: [("00:00", "19:00")],  # Friday
}


def validate_trading_hours(close_time: datetime.datetime) -> bool:
    if ps.ignore_session_ckeck == 1:
        return True

    utc_time = close_time
    day_of_week = utc_time.weekday()  # Monday=0 ... Sunday=6
    current_time = utc_time.time()

    monday_start = datetime.time(12, 0)
    friday_end = datetime.time(12, 0)

    if day_of_week == 0:
        return current_time >= monday_start

    if day_of_week in (1, 2, 3):
        return True

    if day_of_week == 4:
        return current_time <= friday_end

    return False