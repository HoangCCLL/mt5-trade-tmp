"""Streamer: polls MT5 state/ticks/deals and pushes them to the gateway. Never trades."""
import asyncio, logging, os, time
from concurrent.futures import ThreadPoolExecutor
from .common import Link, heartbeat, init_mt5, log
from .mt5_api import mt5

READ_POOL = ThreadPoolExecutor(max_workers=1, thread_name_prefix="read")
STALE_AFTER, STALE_PERIOD, REFRESH_EVERY = 10.0, 5.0, 30.0


def snapshot():
    ti = mt5.terminal_info()
    acc = mt5.account_info()
    return {"account": acc._asdict() if acc else None,
            "positions": [p._asdict() for p in (mt5.positions_get() or ())],
            "connected": bool(ti and ti.connected)}


async def poll_state(emit, period):
    loop, last, sent_at = asyncio.get_running_loop(), None, 0.0
    while True:
        snap = await loop.run_in_executor(READ_POOL, snapshot)
        if snap != last or time.time() - sent_at > REFRESH_EVERY:
            emit("state", snap)
            last, sent_at = snap, time.time()
        await asyncio.sleep(period)


async def poll_ticks(emit, subscribed: set, period):
    loop, last_msc, due = asyncio.get_running_loop(), {}, {}
    while True:
        now = time.time()
        for s in list(subscribed):
            if now < due.get(s, 0):
                continue
            tick = await loop.run_in_executor(READ_POOL, mt5.symbol_info_tick, s)
            if tick and tick.time_msc != last_msc.get(s):
                last_msc[s] = tick.time_msc
                emit("tick", {"symbol": s, "bid": tick.bid, "ask": tick.ask, "time_msc": tick.time_msc})
            stale = tick is None or now - tick.time_msc / 1000 > STALE_AFTER
            due[s] = now + (STALE_PERIOD if stale else 0)  # weekend / illiquid symbol: back off
        await asyncio.sleep(period)


async def poll_deals(emit, period):
    """Deal times come back in the broker's server clock, not UTC, so the query window is int epochs with a
    one-day margin on each side and dedupe is by ticket; the first poll only seeds `seen` (history.fill covers the past)."""
    loop, seen, since, first = asyncio.get_running_loop(), {}, int(time.time()) - 86400, True
    while True:
        deals = await loop.run_in_executor(READ_POOL, mt5.history_deals_get, since, int(time.time()) + 86400)
        for d in deals or ():
            if d.ticket not in seen:
                seen[d.ticket] = int(d.time)
                if not first:
                    emit("deal", d._asdict())
                since = max(since, int(d.time) - 5)
        first = False
        seen = {t: at for t, at in seen.items() if at >= since}
        await asyncio.sleep(period)


async def run(url, token, state_period=0.25, tick_period=0.1, deal_period=1.0):
    init_mt5()
    link, subscribed = Link(url, token), set()

    async def on_message(msg):
        syms = msg["payload"].get("symbols", [])
        if msg["type"] == "subscribe":
            for s in syms:
                mt5.symbol_select(s, True)
            subscribed.update(syms)
        elif msg["type"] == "unsubscribe":
            subscribed.difference_update(syms)

    await asyncio.gather(link.run(on_message), poll_state(link.emit, state_period),
                         poll_ticks(link.emit, subscribed, tick_period), poll_deals(link.emit, deal_period),
                         heartbeat(link.emit, lambda: {"subscribed": sorted(subscribed)}))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(f"{os.environ['GATEWAY_URL']}/agents/{os.environ['ACCOUNT_ID']}/streamer", os.environ["AGENT_TOKEN"]))
