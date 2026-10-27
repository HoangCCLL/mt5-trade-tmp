"""`mt5agent.exe timecheck [SYMBOL]`: measures how the broker clock stamped on ticks/deals relates to this machine's
clock. Run right after opening/closing a trade so the latest deal is fresh; offsets are then the broker-vs-UTC shift."""
import time
from datetime import datetime, timedelta, timezone
from .mt5_api import mt5


def timecheck(symbol="EURUSD"):
    now = time.time()
    mt5.symbol_select(symbol, True)
    tick = mt5.symbol_info_tick(symbol)
    deals = mt5.history_deals_get(int(now) - 7 * 86400, int(now) + 7 * 86400) or ()
    last = max(deals, key=lambda d: d.time, default=None)
    naive = mt5.history_deals_get(datetime.now() - timedelta(seconds=60), datetime.now() + timedelta(seconds=60)) or ()
    wide = mt5.history_deals_get(int(now) - 86400, int(now) + 86400) or ()
    label = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")  # noqa: E731
    return {
        "vps_local_now": datetime.now().strftime("%Y-%m-%d %H:%M:%S %z"),
        "vps_utc_now": label(now),
        "tick_time_as_stamped": label(tick.time) if tick else None,
        "tick_offset_s": round(tick.time - now) if tick else None,
        "deal_ticket": last.ticket if last else None,
        "deal_time_as_stamped": label(last.time) if last else None,
        "deal_offset_s": round(last.time - now) if last else None,
        "deal_in_naive_60s_window": bool(last) and any(d.ticket == last.ticket for d in naive),
        "deal_in_int_1day_window": bool(last) and any(d.ticket == last.ticket for d in wide),
        "deals_last_7d": len(deals),
    }
