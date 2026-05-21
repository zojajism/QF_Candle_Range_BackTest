


import datetime
from typing import Any, Dict, Optional
import logging
import public_moduls as pm
from tick_registry_provider import get_tick_registry
import strategy_modules as sm

logger = logging.getLogger(__name__)

async def tick_process(tick_data: Optional[Dict[str, Any]] = None):

    tick_time = pm.format_time_simple(str(tick_data.get("tick_time")))
    bid = float(tick_data["bid"])
    ask = float(tick_data["ask"])
    symbol = str(tick_data["symbol"])
    
    #print(f"Bid: {bid}, Ask: {ask}, tick_time: {tick_time}")

    #logger.info(f"Tick for {symbol}, tick_time:{parsed_time}")   
    tick_registry = get_tick_registry()
    tick_registry.update_tick("OANDA", symbol, bid, ask, tick_time)

    sm.open_sig_registry.process_tick_for_symbol(
                                exchange="OANDA",
                                symbol=symbol,
                                bid=bid,
                                ask=ask,
                                now=tick_time,
                                conn=None,   # or None if you handle DB elsewhere
                            )