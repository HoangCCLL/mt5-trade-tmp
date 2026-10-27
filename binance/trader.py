"""Binance trader: executes gateway commands against the exchange. asyncio only, no threads."""
import asyncio, logging, os, time, uuid
from collections import OrderedDict
from agent.link import Link, heartbeat, log
from .exchange import BinanceError

now = time.time
results: OrderedDict = OrderedDict()
_symbols_cache = {"at": 0.0, "symbols": []}


def _remember(cid, res):
    results[cid] = res
    while len(results) > 10_000:
        results.popitem(last=False)


def _client_id(p):
    return f"m{int(p.get('magic', 0))}-{uuid.uuid4().hex[:8]}"


async def _algo(ex, symbol, side, kind, stop):
    """closePosition STOP_MARKET / TAKE_PROFIT_MARKET on the opposite side of the position."""
    return await ex.place_order(symbol=symbol, side=side, type=kind, stopPrice=stop, closePosition="true", workingType="MARK_PRICE")


async def _cancel_algos(ex, symbol, kinds):
    for o in await ex.open_orders(symbol):
        if o.get("closePosition") and o["type"] in kinds:
            await ex.cancel_order(symbol, o["orderId"])


async def _position_amt(ex, symbol):
    for p in await ex.positions():
        if p["symbol"] == symbol and float(p["positionAmt"]):
            return float(p["positionAmt"])
    return 0.0


async def execute(ex, cmd) -> dict:
    type_, p = cmd["type"], cmd["payload"]
    p0 = time.perf_counter()
    try:
        if type_ == "symbols.list":
            if now() - _symbols_cache["at"] > 3600:
                info = await ex.exchange_info()
                _symbols_cache.update(at=now(), symbols=sorted(s["symbol"] for s in info["symbols"] if s.get("status") == "TRADING"))
            res = {"status": "done", "symbols": _symbols_cache["symbols"]}
        elif type_ == "order.open":
            side = "BUY" if p["side"] == "buy" else "SELL"
            params = {"symbol": p["symbol"], "side": side, "quantity": p["volume"], "newClientOrderId": _client_id(p),
                      "newOrderRespType": "RESULT"}
            if p.get("type", "market") == "limit":
                params.update(type="LIMIT", timeInForce="GTC", price=p["price"])
            else:
                params["type"] = "MARKET"
            if p.get("reduce_only"):
                params["reduceOnly"] = "true"
            o = await ex.place_order(**params)
            res = {"status": "done", "retcode": 0, "order": o["orderId"], "client": o["clientOrderId"], "order_status": o["status"],
                   "price": float(o["avgPrice"]) or float(o["price"]), "volume": float(o["executedQty"]), "comment": p.get("comment", "")}
            opp = "SELL" if side == "BUY" else "BUY"
            if p.get("sl"):
                res["sl_order"] = (await _algo(ex, p["symbol"], opp, "STOP_MARKET", p["sl"]))["orderId"]
            if p.get("tp"):
                res["tp_order"] = (await _algo(ex, p["symbol"], opp, "TAKE_PROFIT_MARKET", p["tp"]))["orderId"]
        elif type_ == "order.close":
            symbol, amt = p["position"], await _position_amt(ex, p["position"])
            if not amt:
                return {"status": "error", "retcode": 0, "comment": "position_not_found"}
            qty = min(float(p.get("volume") or abs(amt)), abs(amt))
            o = await ex.place_order(symbol=symbol, side="SELL" if amt > 0 else "BUY", type="MARKET", quantity=qty,
                                     reduceOnly="true", newOrderRespType="RESULT", newClientOrderId=_client_id(p))
            res = {"status": "done", "retcode": 0, "order": o["orderId"], "price": float(o["avgPrice"]), "volume": float(o["executedQty"])}
            if qty >= abs(amt):
                await _cancel_algos(ex, symbol, {"STOP_MARKET", "TAKE_PROFIT_MARKET"})
        elif type_ == "order.modify":
            symbol, amt = p["position"], await _position_amt(ex, p["position"])
            if not amt:
                return {"status": "error", "retcode": 0, "comment": "position_not_found"}
            opp = "SELL" if amt > 0 else "BUY"
            res = {"status": "done", "retcode": 0}
            for key, kind in (("sl", "STOP_MARKET"), ("tp", "TAKE_PROFIT_MARKET")):
                if p.get(key) is not None:
                    await _cancel_algos(ex, symbol, {kind})
                    res[f"{key}_order"] = (await _algo(ex, symbol, opp, kind, p[key]))["orderId"]
        elif type_ == "order.cancel":
            o = await ex.cancel_order(p["symbol"], p["order"])
            res = {"status": "done", "retcode": 0, "order": o["orderId"], "order_status": o["status"]}
        else:
            res = {"status": "error", "retcode": 0, "comment": f"unknown command {type_}"}
    except BinanceError as e:
        res = {"status": "error", "retcode": e.code, "comment": e.msg}
    res["d_mt5"] = time.perf_counter() - p0
    return res


async def handle_command(ex, cmd, emit, ttl, order_timeout, skew=0.0):
    cid, t_recv, p_recv = cmd["id"], now(), time.perf_counter()
    if cid in results:
        emit("result", results[cid])
        return
    if t_recv + skew - float(cmd.get("ts", 0)) > ttl:
        res = {"id": cid, "status": "rejected", "comment": "ttl_expired"}
        _remember(cid, res)
        emit("result", res)
        return
    emit("ack", {"id": cid})
    task = asyncio.ensure_future(execute(ex, cmd))
    try:
        res = await asyncio.wait_for(asyncio.shield(task), order_timeout)
    except asyncio.TimeoutError:
        emit("result", {"id": cid, "status": "unknown", "t_recv": t_recv})
        res = await task
    except Exception as e:
        log.exception("execute failed for %s", cid)
        res = {"status": "error", "retcode": -1, "comment": f"{type(e).__name__}: {e}"}
    res = {"id": cid, "t_recv": t_recv, "t_skew": skew, "d_pool": 0.0, **res, "t_sent": now()}
    _remember(cid, res)
    emit("result", res)


def skew_estimate(ts, echo, now_, prev=None, alpha=0.2):
    sample = ts - (echo + (now_ - echo) / 2) if echo is not None else ts - now_
    return sample if prev is None else prev + alpha * (sample - prev)


async def run(url, token, ex, order_timeout=30.0, ttl=5.0):
    link, clock = Link(url, token), {"skew": 0.0, "est": None}

    async def on_message(msg):
        if msg["type"] == "hello":
            p = msg["payload"]
            clock["skew"] = clock["est"] = skew_estimate(p["ts"], p.get("echo"), now(), clock["est"])
            return
        await handle_command(ex, msg, link.emit, ttl, order_timeout, skew=clock["skew"])

    await asyncio.gather(link.run(on_message),
                         heartbeat(link.emit, lambda: {"connected": True, "clock_skew": round(clock["skew"], 3),
                                                       "used_weight": getattr(ex, "used_weight", 0), "t": now()}))
