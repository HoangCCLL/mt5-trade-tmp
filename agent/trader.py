"""Trader: executes commands from the gateway on MT5. No polling. Every mt5.* call runs in TRADE_POOL."""
import asyncio, logging, os, time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from .common import Link, heartbeat, init_mt5, log, mt5_connected
from .mt5_api import mt5

now = time.time  # patched in tests; TTL is checked in gateway time via the skew measured from `hello`
TRADE_POOL = ThreadPoolExecutor(max_workers=4, thread_name_prefix="trade")
BATCH = 50
results: OrderedDict[str, dict] = OrderedDict()
_symbol_cache: dict[str, object] = {}


def _remember(cid, res):
    results[cid] = res
    if len(results) > 10_000:
        results.popitem(last=False)


def _symbol(symbol):
    if symbol not in _symbol_cache:
        mt5.symbol_select(symbol, True)
        _symbol_cache[symbol] = mt5.symbol_info(symbol)
    return _symbol_cache[symbol]


SYMBOL_FILLING_FOK, SYMBOL_FILLING_IOC = 1, 2  # MQL5 bit flags; the Python SDK does not export them


def _filling(info):
    fm = info.filling_mode
    if fm & SYMBOL_FILLING_FOK:
        return mt5.ORDER_FILLING_FOK
    if fm & SYMBOL_FILLING_IOC:
        return mt5.ORDER_FILLING_IOC
    return mt5.ORDER_FILLING_RETURN


def build_request(type_, p):
    if type_ == "order.open":
        info, tick = _symbol(p["symbol"]), mt5.symbol_info_tick(p["symbol"])
        buy = p["side"] == "buy"
        return {"action": mt5.TRADE_ACTION_DEAL, "symbol": p["symbol"], "volume": float(p["volume"]),
                "type": mt5.ORDER_TYPE_BUY if buy else mt5.ORDER_TYPE_SELL,
                "price": tick.ask if buy else tick.bid, "sl": float(p.get("sl") or 0), "tp": float(p.get("tp") or 0),
                "deviation": int(p.get("deviation", 20)), "magic": int(p["magic"]), "comment": p.get("comment") or "",
                "type_time": mt5.ORDER_TIME_GTC, "type_filling": _filling(info)}
    pos = mt5.positions_get(ticket=int(p["position"]))
    if not pos:
        return None
    pos = pos[0]
    if type_ == "order.modify":
        return {"action": mt5.TRADE_ACTION_SLTP, "position": pos.ticket, "symbol": pos.symbol,
                "sl": float(p.get("sl") if p.get("sl") is not None else pos.sl),
                "tp": float(p.get("tp") if p.get("tp") is not None else pos.tp)}
    if type_ == "order.close":
        info, tick = _symbol(pos.symbol), mt5.symbol_info_tick(pos.symbol)
        closing_buy = pos.type == mt5.ORDER_TYPE_SELL
        return {"action": mt5.TRADE_ACTION_DEAL, "position": pos.ticket, "symbol": pos.symbol,
                "volume": float(p.get("volume") or pos.volume),
                "type": mt5.ORDER_TYPE_BUY if closing_buy else mt5.ORDER_TYPE_SELL,
                "price": tick.ask if closing_buy else tick.bid, "deviation": int(p.get("deviation", 20)),
                "magic": pos.magic, "type_time": mt5.ORDER_TIME_GTC, "type_filling": _filling(info)}
    raise ValueError(type_)


scheduled: dict[str, asyncio.Task] = {}   # scheduled exits (order.close when=market_open) by command id
GAP_MS = 20 * 3600 * 1000                  # a tick this long after the previous one = first tick after the weekly close
RETRY_CODES = {10004, 10018, 10021, 10024}  # requote, market closed, off quotes, too many requests


def execute(cmd) -> dict:
    """Runs in a worker thread. Returns the result payload (without id)."""
    cmd["_p_start"] = time.perf_counter()  # thread actually started: end of the pool wait
    res = _execute(cmd)
    res["p_start"], res["d_mt5"] = cmd.get("_p_start"), time.perf_counter() - cmd.get("_p_start", time.perf_counter())
    return res


def _execute(cmd) -> dict:
    type_, p = cmd["type"], cmd["payload"]
    if type_ == "symbols.list":  # Market Watch names; the real SDK also exposes hidden symbols, keep only visible ones
        syms = mt5.symbols_get() or ()
        return {"status": "done", "symbols": sorted(x.name for x in syms if getattr(x, "visible", True))}
    if type_ == "symbol.spec":
        info = _symbol(p["symbol"])
        if info is None:
            return {"status": "error", "retcode": 0, "comment": "unknown symbol"}
        return {"status": "done", "spec": {"symbol": info.name, "base": info.currency_base, "quote": info.currency_profit,
                                           "contract_size": info.trade_contract_size, "volume_min": info.volume_min,
                                           "volume_step": info.volume_step, "volume_max": info.volume_max,
                                           "price_step": info.point, "min_notional": 0}}
    if type_ == "symbol.info":
        return {"status": "done", "symbols": {s: mt5.symbol_info_tick(s)._asdict() for s in p["symbols"]}}
    req = build_request(type_, p)
    if req is None:
        return {"status": "error", "retcode": 0, "comment": "position_not_found"}
    chk = mt5.order_check(req)
    if chk.retcode not in (0, mt5.TRADE_RETCODE_DONE):
        return {"status": "error", "retcode": chk.retcode, "comment": chk.comment}
    r = mt5.order_send(req)
    if r is None:
        return {"status": "error", "retcode": -1, "comment": str(mt5.last_error())}
    if r.retcode == mt5.TRADE_RETCODE_INVALID_FILL:
        _symbol_cache.pop(req["symbol"], None)  # refresh filling mode next time
    return {"status": "done" if r.retcode == mt5.TRADE_RETCODE_DONE else "error", "retcode": r.retcode,
            "comment": r.comment, "order": r.order, "deal": r.deal, "price": r.price, "volume": r.volume}


async def history_fill(cmd, emit):
    p = cmd["payload"]
    loop = asyncio.get_running_loop()
    # int epochs pass through as broker server time; +1 day absorbs the server-vs-VPS clock offset
    deals = await loop.run_in_executor(TRADE_POOL, mt5.history_deals_get,
                                       int(float(p.get("from") or 0)), int(now()) + 86400)
    deals = [d._asdict() for d in deals]
    total = len(deals)
    for i in range(0, total, BATCH):
        emit("history", {"batch": i // BATCH + 1, "total": total, "deals": deals[i:i + BATCH]})
        await asyncio.sleep(0.1)
    return {"status": "done", "count": total}


def skew_estimate(ts, echo, now_, prev=None, alpha=0.2):
    """Gateway clock minus agent clock. `ts` was stamped by the gateway when it received the heartbeat the agent sent
    at `echo`; the hello arrived at `now_`, so the gateway stamped it about half the round trip after `echo`.
    Without that correction the estimate is off by the one-way delay. Smoothed to ride out jitter."""
    sample = ts - (echo + (now_ - echo) / 2) if echo is not None else ts - now_
    return sample if prev is None else prev + alpha * (sample - prev)


async def wait_market_open(cmd, emit):
    """Scheduled exit: poll the broker tick; the first tick that is GAP_MS after the previous one is the weekly open.
    Then close at market, retrying transient retcodes. Clock-independent: compares broker tick times with each other."""
    cid, p, loop = cmd["id"], cmd["payload"], asyncio.get_running_loop()
    ticket, t_recv, prev, attempts, first_tick, res = p["position"], now(), None, 0, None, None
    try:
        while res is None:
            if not mt5_connected():
                await asyncio.sleep(1)
                continue
            pos = await loop.run_in_executor(TRADE_POOL, lambda: mt5.positions_get(ticket=ticket))
            if not pos:
                res = {"status": "done", "retcode": 0, "comment": "position_not_found"}
                break
            tick = await loop.run_in_executor(TRADE_POOL, mt5.symbol_info_tick, pos[0].symbol)
            if tick is None:
                await asyncio.sleep(0.5)
                continue
            if first_tick is None and prev is not None and tick.time_msc - prev >= GAP_MS:
                first_tick = tick.time_msc
                emit("update", {"id": cid, "state": "firing", "t_first_tick": first_tick})
            prev = tick.time_msc
            if first_tick is None:
                await asyncio.sleep(0.1)
                continue
            info = _symbol(pos[0].symbol)
            spread = round((tick.ask - tick.bid) / info.point) if info and info.point else 0
            if p.get("max_spread_points") and spread > p["max_spread_points"]:
                await asyncio.sleep(0.1)
                continue
            attempts += 1
            r = await loop.run_in_executor(TRADE_POOL, execute, {"type": "order.close", "payload": {"position": ticket, "deviation": p.get("deviation", 50)}})
            if r["status"] == "done" or r.get("retcode") not in RETRY_CODES or attempts >= 4000:   # ~10 min of retries
                res = {**r, "spread_points": spread}
                break
            if attempts <= 5 or attempts % 20 == 0:
                emit("update", {"id": cid, "state": "retrying", "attempts": attempts, "retcode": r.get("retcode")})
            await asyncio.sleep(0.15)
    except asyncio.CancelledError:
        res = {"status": "canceled", "retcode": 0}
    finally:
        scheduled.pop(cid, None)
    res = {"id": cid, "when": "market_open", "t_recv": t_recv, "attempts": attempts, "t_first_tick": first_tick, **res, "t_sent": now()}
    _remember(cid, res)
    emit("result", res)


async def handle_command(cmd, emit, ttl, order_timeout, skew=0.0):
    cid, t_recv = cmd["id"], now()
    if cid in results:
        emit("result", results[cid])
        return
    if cmd["type"] == "order.close" and cmd["payload"].get("when") == "market_open":
        emit("ack", {"id": cid})
        if cid not in scheduled:   # re-sent by the gateway after a reconnect: keep the running one
            scheduled[cid] = asyncio.create_task(wait_market_open(cmd, emit))
            emit("update", {"id": cid, "state": "armed", "position": cmd["payload"]["position"]})
        return
    if cmd["type"] == "schedule.cancel":
        emit("ack", {"id": cid})
        task = scheduled.pop(cmd["payload"]["id"], None)
        if task:
            task.cancel()
        res = {"id": cid, "status": "done", "retcode": 0, "canceled": task is not None, "t_recv": t_recv, "t_sent": now()}
        _remember(cid, res)
        emit("result", res)
        return
    if t_recv + skew - float(cmd.get("ts", 0)) > ttl:  # compare in gateway time
        res = {"id": cid, "status": "rejected", "comment": "ttl_expired"}
        _remember(cid, res)
        emit("result", res)
        return
    emit("ack", {"id": cid})
    if cmd["type"] == "history.fill":
        res = await history_fill(cmd, emit)
    else:
        p_recv = time.perf_counter()
        fut = asyncio.get_running_loop().run_in_executor(TRADE_POOL, execute, cmd)
        try:
            res = await asyncio.wait_for(asyncio.shield(fut), order_timeout)
        except asyncio.TimeoutError:
            emit("result", {"id": cid, "status": "unknown", "t_recv": t_recv})
            res = await fut
        except Exception as e:
            log.exception("execute failed for %s", cid)
            res = {"status": "error", "retcode": -1, "comment": f"{type(e).__name__}: {e}"}  # thread keeps running; report the real outcome when it lands
    if res.get("p_start") is not None:
        res["d_pool"] = res.pop("p_start") - p_recv  # monotonic: immune to the 15.6 ms Windows wall-clock tick
    res = {"id": cid, "t_recv": t_recv, "t_skew": skew, **res, "t_sent": now()}  # agent clock; gateway builds the latency breakdown
    _remember(cid, res)
    emit("result", res)


async def run(url, token, order_timeout=30.0, ttl=5.0):
    init_mt5()
    link, clock = Link(url, token), {"skew": 0.0}

    async def on_message(msg):
        if msg["type"] == "hello":
            p = msg["payload"]
            skew = skew_estimate(p["ts"], p.get("echo"), now(), clock.get("est"))
            if clock.get("est") is None or abs(skew - clock["skew"]) > 1.0:  # hello arrives every heartbeat; log real changes
                log.info("clock skew vs gateway: %+.3fs", skew)
            clock["skew"] = clock["est"] = skew
            return
        await handle_command(msg, link.emit, ttl, order_timeout, skew=clock["skew"])

    await asyncio.gather(link.run(on_message),
                         heartbeat(link.emit, lambda: {"connected": mt5_connected(), "pool_busy": TRADE_POOL._work_queue.qsize(),
                                                       "clock_skew": round(clock["skew"], 3), "t": now()}))  # t echoed in hello


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(run(f"{os.environ['GATEWAY_URL']}/agents/{os.environ['ACCOUNT_ID']}/trader", os.environ["AGENT_TOKEN"],
                    order_timeout=float(os.environ.get("ORDER_TIMEOUT", 30)), ttl=float(os.environ.get("CMD_TTL", 5))))
