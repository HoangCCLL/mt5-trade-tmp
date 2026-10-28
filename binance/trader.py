"""Binance trader: executes gateway commands against the exchange. asyncio only, no threads."""
import asyncio, logging, os, time, uuid
from collections import OrderedDict
from agent.link import Link, heartbeat, log
from .exchange import BinanceError

now = time.time
results: OrderedDict = OrderedDict()
chases: dict[str, asyncio.Event] = {}   # running chase command id -> cancel flag
EPS = 1e-9
POLL = 0.25   # seconds between fill checks while a chase order rests


def _remember(cid, res):
    results[cid] = res
    while len(results) > 10_000:
        results.popitem(last=False)


def _fill(o):
    """Normalise a place/get order response: Binance omits avgPrice/executedQty in some ACK-style responses."""
    return {"order": o["orderId"], "client": o.get("clientOrderId"), "order_status": o.get("status"),
            "price": float(o.get("avgPrice") or 0) or float(o.get("price") or 0), "volume": float(o.get("executedQty") or 0)}


async def _place(ex, **params):
    """Place and, if the response does not carry the fill, read the order back once."""
    o = await ex.place_order(**params)
    if "executedQty" not in o or "avgPrice" not in o:
        log.info("thin order response %s, reading back", o)
        o = await ex.get_order(params["symbol"], o["orderId"])
    return o


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


async def _symbols(ex):
    """exchangeInfo cached for an hour, per exchange instance."""
    cache = getattr(ex, "_symcache", None)
    if cache is None or now() - cache["at"] > 3600:
        info = await ex.exchange_info()
        cache = ex._symcache = {"at": now(), "info": {s["symbol"]: s for s in info["symbols"]},
                                "symbols": sorted(s["symbol"] for s in info["symbols"] if s.get("status") == "TRADING")}
    return cache


def _spec(sym):
    """Exchange-neutral symbol spec (same schema as the MT5 agent): quantity is in units of `base`, 1 unit per contract."""
    f = {x["filterType"]: x for x in sym.get("filters", [])}
    lot, pf, mn = f.get("LOT_SIZE", {}), f.get("PRICE_FILTER", {}), f.get("MIN_NOTIONAL", {})
    return {"symbol": sym["symbol"], "base": sym["baseAsset"], "quote": sym["quoteAsset"], "contract_size": 1,
            "volume_min": float(lot.get("minQty", 0)), "volume_step": float(lot.get("stepSize", 0)), "volume_max": float(lot.get("maxQty", 0)),
            "price_step": float(pf.get("tickSize", 0)), "min_notional": float(mn.get("notional", 0))}


async def chase(ex, cmd, emit) -> dict:
    """Post-only limit at the best price on our side, moved whenever that price changes (or every interval_ms),
    until filled or canceled by `chase.cancel`. Partial fills are kept; only the remainder is re-quoted."""
    p, cid = cmd["payload"], cmd["id"]
    symbol, side = p["symbol"], "BUY" if p["side"] == "buy" else "SELL"
    interval = float(p.get("interval_ms", 500)) / 1000
    remaining, filled, cost, moves, t0 = float(p["volume"]), 0.0, 0.0, 0, time.perf_counter()
    cancel = chases[cid] = asyncio.Event()
    best, changed = {}, asyncio.Event()
    prefix = f"m{int(p.get('magic', 0))}-"
    try:   # a previous agent process may have left this leg's order resting: take its fills, cancel the rest
        for o in await ex.open_orders(symbol):
            if str(o.get("clientOrderId", "")).startswith(prefix):
                try:
                    o = await ex.cancel_order(symbol, o["orderId"])
                except BinanceError as e:
                    if e.code != -2011:
                        raise
                    o = await ex.get_order(symbol, o["orderId"])
                got = float(o.get("executedQty") or 0)
                if got:
                    filled, cost, remaining = round(filled + got, 8), cost + got * float(o.get("avgPrice") or o.get("price") or 0), round(remaining - got, 8)
                    log.info("chase %s adopted %s filled from orphan order %s", cid, got, o["orderId"])
    except BinanceError as e:
        log.warning("orphan cleanup failed: %r", e)

    async def ticker():
        while True:
            try:
                async for t in ex.book_ticker([symbol]):
                    best.update(bid=float(t["b"]), ask=float(t["a"]))
                    changed.set()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("bookTicker error during chase: %r", e)
                await asyncio.sleep(1)

    tk = asyncio.create_task(ticker())
    my_side = "bid" if side == "BUY" else "ask"
    try:
        while remaining > EPS and not cancel.is_set():
            params = {"symbol": symbol, "side": side, "type": "LIMIT", "timeInForce": "GTX", "priceMatch": "QUEUE",
                      "quantity": round(remaining, 8), "newClientOrderId": _client_id(p)}
            if p.get("reduce_only"):
                params["reduceOnly"] = "true"
            try:
                o = await ex.place_order(**params)
            except BinanceError as e:
                if e.code == -5022:       # would take liquidity: the book moved into us, re-quote at the new queue
                    moves += 1
                    await asyncio.sleep(0.05)
                    continue
                if e.code == -1015:       # order rate limit
                    await asyncio.sleep(2)
                    continue
                raise
            if o.get("status") == "EXPIRED":
                moves += 1
                continue
            price, ref = float(o["price"]), best.get(my_side)
            emit("update", {"id": cid, "turn": p.get("turn"), "leg": p.get("leg"), "order": o["orderId"], "price": price,
                            "best_bid": best.get("bid"), "best_ask": best.get("ask"), "filled": filled, "remaining": remaining,
                            "moves": moves, "elapsed_ms": round((time.perf_counter() - t0) * 1000)})
            deadline, last_poll, cur = time.perf_counter() + interval, time.perf_counter(), o
            while not cancel.is_set():
                changed.clear()
                left = deadline - time.perf_counter()
                if left <= 0:
                    want = best.get(my_side)
                    if want is None or abs(want - price) < 1e-9:   # still at the queue: keep our place, just re-check later
                        deadline = time.perf_counter() + interval
                        continue
                    break
                try:
                    await asyncio.wait_for(changed.wait(), min(left, POLL))
                except asyncio.TimeoutError:
                    pass
                if best.get(my_side) != ref and abs((best.get(my_side) or price) - price) >= 1e-9:
                    break
                if time.perf_counter() - last_poll >= POLL:   # fill detection: REST poll (ponytail: user stream later)
                    last_poll = time.perf_counter()
                    cur = await ex.get_order(symbol, o["orderId"])
                    if cur["status"] in ("FILLED", "CANCELED", "EXPIRED"):
                        break
            if cur.get("status") not in ("FILLED", "CANCELED", "EXPIRED"):
                cur = await ex.get_order(symbol, o["orderId"])
            if cur["status"] not in ("FILLED", "CANCELED", "EXPIRED"):
                try:
                    cur = await ex.cancel_order(symbol, o["orderId"])
                except BinanceError as e:
                    if e.code != -2011:
                        raise
                    cur = await ex.get_order(symbol, o["orderId"])   # filled while we were canceling
            got = float(cur.get("executedQty") or 0)
            if got:
                filled, cost, remaining = round(filled + got, 8), cost + got * float(cur.get("avgPrice") or price), round(remaining - got, 8)
            if remaining > EPS and not cancel.is_set():
                moves += 1
    finally:
        tk.cancel()
        chases.pop(cid, None)
    return {"status": "done" if remaining <= EPS else "canceled", "retcode": 0, "filled": filled, "remaining": max(remaining, 0.0),
            "avg_price": round(cost / filled, 8) if filled else None, "volume": filled, "price": round(cost / filled, 8) if filled else None,
            "moves": moves, "elapsed_ms": round((time.perf_counter() - t0) * 1000), "turn": p.get("turn"), "leg": p.get("leg")}


async def execute(ex, cmd) -> dict:
    type_, p = cmd["type"], cmd["payload"]
    p0 = time.perf_counter()
    try:
        if type_ == "symbols.list":
            res = {"status": "done", "symbols": (await _symbols(ex))["symbols"]}
        elif type_ == "symbol.spec":
            sym = (await _symbols(ex))["info"].get(p["symbol"])
            res = {"status": "done", "spec": _spec(sym)} if sym else {"status": "error", "retcode": 0, "comment": "unknown symbol"}
        elif type_ == "account.leverage":
            r = await ex.set_leverage(p["symbol"], p["leverage"])
            res = {"status": "done", "retcode": 0, "leverage": int(r.get("leverage", p["leverage"]))}
        elif type_ == "chase.cancel":
            ev = chases.get(p["id"])
            if ev:
                ev.set()
            res = {"status": "done", "retcode": 0, "canceled": ev is not None}
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
            o = await _place(ex, **params)
            res = {"status": "done", "retcode": 0, **_fill(o), "comment": p.get("comment", "")}
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
            o = await _place(ex, symbol=symbol, side="SELL" if amt > 0 else "BUY", type="MARKET", quantity=qty,
                             reduceOnly="true", newOrderRespType="RESULT", newClientOrderId=_client_id(p))
            res = {"status": "done", "retcode": 0, **_fill(o)}
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
    try:
        if cmd["type"] == "order.chase":   # long-running by design: no order_timeout, ends on fill or chase.cancel
            if cid in chases:              # re-sent by the gateway after a reconnect while this chase is still alive
                return
            res = await chase(ex, cmd, emit)
        else:
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
