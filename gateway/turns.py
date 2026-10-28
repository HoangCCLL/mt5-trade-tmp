"""Trade turn orchestrator (docs/trade-turn-spec.md): two Binance maker legs + two MT5 market hedges per turn.
State lives in Redis (turn:{id}) and in this process; single uvicorn worker. Agents run the latency loops
(chase, weekly-open exit); this module only decides what to send next and records what came back."""
import json, math, time, uuid
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from . import bus, config
from .api import api_key
from .registry import registry, TraderOffline, ResultTimeout

MAX_OPEN = 20
LEGS = ("long_in", "short_in", "mt5_short", "mt5_long", "long_out", "short_out")
SIDES = {"long": ("buy", "sell"), "short": ("sell", "buy")}   # binance in-side, out-side
HEDGE = {"long": "mt5_short", "short": "mt5_long"}           # binance side -> mt5 leg (opposite direction)
DONE = ("closed", "force_closed", "canceled")

turns: dict[int, dict] = {}
cmd_index: dict[str, tuple[int, str, str]] = {}   # command id -> (turn id, leg, kind)
router = APIRouter(dependencies=[Depends(api_key)])


def S():
    return config.TURN_SET


def _leg():
    return {"status": "pending", "filled": 0.0, "qty": None, "price": None, "moves": 0, "best_bid": None, "best_ask": None,
            "ticket": None, "order": None, "cmd": None, "kind": None, "exit": None, "error": None, "updated": time.time()}


def _status(t):
    L = t["legs"]
    ins, outs, mts = [L["long_in"], L["short_in"]], [L["long_out"], L["short_out"]], [L["mt5_short"], L["mt5_long"]]
    busy = any(l["status"] in ("chasing", "sending", "closing", "force_closing") for l in L.values())
    if t.get("forced"):
        return "force_closing" if busy else "force_closed"
    if any(l["status"] == "failed" for l in L.values()):
        return "partial"
    if all(i["status"] == "canceled" for i in ins):
        return "canceled"
    if any(i["status"] == "chasing" for i in ins) or any(m["status"] == "sending" for m in mts):
        return "opening"
    if any(o["status"] == "chasing" for o in outs) or any(m["status"] == "closing" for m in mts):
        return "closing"
    binance_out = all(i["status"] != "filled" for i in ins)          # every filled side has been exited (or never filled)
    mt5_out = all(m["status"] in ("pending", "closed") for m in mts)
    if binance_out and mt5_out:
        return "closed"
    if binance_out:
        return "mt5_only"   # Binance exited, MT5 hedges still open: WMO territory
    return "open"


async def _save(t):
    t["status"] = _status(t)
    t["updated"] = time.time()
    await bus.r.set(f"turn:{t['id']}", json.dumps(t))
    await bus.r.sadd("turns", t["id"])
    await bus.publish("turns", "turn", t)


async def load():
    turns.clear(); cmd_index.clear()
    for tid in await bus.r.smembers("turns"):
        raw = await bus.r.get(f"turn:{int(tid)}")
        if not raw:
            continue
        t = json.loads(raw)
        turns[t["id"]] = t
        for leg, l in t["legs"].items():
            if l.get("cmd"):
                cmd_index[l["cmd"]] = (t["id"], leg, l.get("kind"))
        for leg, sid in (t.get("wmo_ids") or {}).items():
            cmd_index[sid] = (t["id"], leg, "wmo")


async def _send(acct, type_, payload, t, leg, kind, status):
    l = t["legs"][leg]
    try:
        cid = await registry.send_command(acct, type_, payload, wait=False)
    except TraderOffline:
        l.update(status="failed", error=f"{acct}: trader offline", updated=time.time())
        return None
    cmd_index[cid] = (t["id"], leg, kind)
    l.update(cmd=cid, kind=kind, status=status, error=None, updated=time.time())
    return cid


async def _cancel_chase(acct, l):
    try:
        await registry.send_command(acct, "chase.cancel", {"id": l["cmd"]})
    except (TraderOffline, ResultTimeout) as e:
        l["error"] = f"cancel: {e!r}"


# ---- volume conversion through the physical unit of the asset

def _round_down(x, step):
    return round(math.floor(x / step + 1e-9) * step, 10) if step else x


async def convert(units: float):
    sb = await registry.send_command(S()["long"], "symbol.spec", {"symbol": S()["symbol_binance"]})
    sm = await registry.send_command(S()["mt5"], "symbol.spec", {"symbol": S()["symbol_mt5"]})
    if sb.get("status") != "done" or sm.get("status") != "done":
        raise HTTPException(502, f"spec unavailable: {sb.get('comment') or sm.get('comment')}")
    b, m = sb["spec"], sm["spec"]
    qb, qm = _round_down(units / b["contract_size"], b["volume_step"]), _round_down(units / m["contract_size"], m["volume_step"])
    reason = None
    if b["base"] != m["base"]:
        reason = f"base mismatch: binance {b['base']} vs mt5 {m['base']}"
    elif qb < b["volume_min"] or (b["volume_max"] and qb > b["volume_max"]):
        reason = f"binance volume {qb} outside [{b['volume_min']}, {b['volume_max']}]"
    elif qm < m["volume_min"] or (m["volume_max"] and qm > m["volume_max"]):
        reason = f"mt5 volume {qm} outside [{m['volume_min']}, {m['volume_max']}]"
    return {"units": units, "base": b["base"], "binance": qb, "mt5": qm, "ok": reason is None, "reason": reason, "specs": {"binance": b, "mt5": m}}


# ---- actions

async def start(units, leverage, interval_ms):
    if not S():
        raise HTTPException(503, "TURN_SET not configured")
    lo, hi = S().get("leverage", [1, 125])
    if not lo <= leverage <= hi:
        raise HTTPException(422, f"leverage must be within [{lo}, {hi}]")
    if sum(1 for t in turns.values() if t["status"] not in DONE) >= MAX_OPEN:
        raise HTTPException(409, f"max {MAX_OPEN} open turns")
    conv = await convert(units)
    if not conv["ok"]:
        raise HTTPException(422, conv["reason"])
    for acct in (S()["long"], S()["short"]):
        try:
            r = await registry.send_command(acct, "account.leverage", {"symbol": S()["symbol_binance"], "leverage": leverage})
        except (TraderOffline, ResultTimeout) as e:
            raise HTTPException(503, f"{acct}: {e!r}")
        if r.get("status") != "done":
            raise HTTPException(502, f"{acct}: leverage {r.get('comment')}")
    tid = int(await bus.r.incr("turn:seq"))
    t = {"id": tid, "created": time.time(), "status": "opening", "forced": False, "wmo": "off", "wmo_ids": {},
         "symbol": {"binance": S()["symbol_binance"], "mt5": S()["symbol_mt5"]}, "volume": {"binance": conv["binance"], "mt5": conv["mt5"]},
         "units": units, "base": conv["base"], "leverage": leverage, "interval_ms": interval_ms, "legs": {l: _leg() for l in LEGS}}
    turns[tid] = t
    for side in ("long", "short"):
        t["legs"][f"{side}_in"]["qty"] = conv["binance"]
        await _send(S()[side], "order.chase", {"symbol": S()["symbol_binance"], "side": SIDES[side][0], "volume": conv["binance"],
                                               "interval_ms": interval_ms, "magic": tid, "turn": tid, "leg": f"{side}_in"},
                    t, f"{side}_in", "in", "chasing")
    await _save(t)
    return t


async def _open_hedge(t, side):
    leg = HEDGE[side]
    qty = t["volume"]["mt5"]
    t["legs"][leg]["qty"] = qty
    await _send(S()["mt5"], "order.open", {"symbol": S()["symbol_mt5"], "side": SIDES[side][1], "volume": qty, "magic": t["id"],
                                           "comment": f"t{t['id']}-{leg}"}, t, leg, "mt5_open", "sending")


async def _exit_binance(t, side, force=False):
    L, acct = t["legs"], S()[side]
    i, o = L[f"{side}_in"], L[f"{side}_out"]
    if i["status"] == "chasing":
        await _cancel_chase(acct, i)   # nothing to exit yet; the chase result will mark it canceled
        return
    if o["status"] == "chasing":
        if not force:
            return
        await _cancel_chase(acct, o)
    if i["status"] != "filled":
        return
    remaining = round(i["filled"] - o["filled"], 8)
    if remaining <= 0:
        return
    if force:
        i["exit"] = "market"
        await _send(acct, "order.close", {"position": S()["symbol_binance"], "volume": remaining}, t, f"{side}_in", "force", "force_closing")
    else:
        o["qty"] = remaining
        await _send(acct, "order.chase", {"symbol": S()["symbol_binance"], "side": SIDES[side][1], "volume": remaining, "reduce_only": True,
                                          "interval_ms": t["interval_ms"], "magic": t["id"], "turn": t["id"], "leg": f"{side}_out"},
                    t, f"{side}_out", "out", "chasing")


async def _exit_mt5(t, leg):
    l = t["legs"][leg]
    if l["status"] == "open":
        await _send(S()["mt5"], "order.close", {"position": l["ticket"], "deviation": 50}, t, leg, "mt5_close", "closing")


async def close(tid, legs=None):
    t = _get(tid)
    for side in ("long", "short"):
        out = t["legs"][f"{side}_out"]
        if legs and f"{side}_out" in legs and out["status"] == "chasing":
            await _cancel_chase(S()[side], out)   # "close" on a chasing exit leg = stop chasing, keep the position
        elif legs is None or f"{side}_in" in legs or f"{side}_out" in legs:
            await _exit_binance(t, side)
    for leg in ("mt5_short", "mt5_long"):
        if legs is None or leg in legs:
            await _exit_mt5(t, leg)
    await _save(t)
    return t


async def force_close(tid):
    t = _get(tid)
    t["forced"] = True
    for side in ("long", "short"):
        await _exit_binance(t, side, force=True)
    for leg in ("mt5_short", "mt5_long"):
        await _exit_mt5(t, leg)
    if t["wmo"] == "armed":
        await wmo(tid, False, save=False)
    await _save(t)
    return t


async def wmo(tid, on, save=True):
    t = _get(tid)
    L = t["legs"]
    if on:
        binance_out = all(L[f"{s}_in"]["status"] != "filled" for s in ("long", "short")) and any(L[f"{s}_out"]["status"] == "filled" for s in ("long", "short"))
        open_mt5 = [leg for leg in ("mt5_short", "mt5_long") if L[leg]["status"] == "open"]
        if not binance_out or not open_mt5:
            raise HTTPException(409, "WMO needs both Binance legs exited and MT5 hedges still open")
        for leg in open_mt5:
            sid = str(uuid.uuid4())
            payload = {"position": L[leg]["ticket"], "when": "market_open", "deviation": 50}
            await bus.sched_put(S()["mt5"], sid, payload)
            try:
                await registry.send_command(S()["mt5"], "order.close", payload, id_=sid, wait=False)
            except TraderOffline:
                pass   # armed when the trader reconnects
            cmd_index[sid] = (tid, leg, "wmo")
            t["wmo_ids"][leg] = sid
        t["wmo"] = "armed"
    else:
        for leg, sid in list(t["wmo_ids"].items()):
            await bus.sched_del(S()["mt5"], sid)
            try:
                await registry.send_command(S()["mt5"], "schedule.cancel", {"id": sid})
            except (TraderOffline, ResultTimeout):
                pass
            cmd_index.pop(sid, None)
        t["wmo_ids"] = {}
        t["wmo"] = "off"
    if save:
        await _save(t)
    return t


def _get(tid):
    t = turns.get(tid)
    if not t:
        raise HTTPException(404, "turn not found")
    return t


# ---- agent events (called from ws.on_trader)

async def on_event(acct, type_, p):
    ref = cmd_index.get(p.get("id"))
    if not ref:
        return
    tid, leg, kind = ref
    t = turns.get(tid)
    if not t:
        return
    l = t["legs"][leg]
    if type_ == "update":
        if kind in ("in", "out"):
            l.update(price=p.get("price"), best_bid=p.get("best_bid"), best_ask=p.get("best_ask"), moves=p.get("moves", l["moves"]),
                     filled=p.get("filled", l["filled"]), order=p.get("order"), updated=time.time())
        elif kind == "wmo" and p.get("state") == "firing":
            t["wmo"] = "firing"
        await _save(t)
        return
    if type_ != "result" or p.get("status") == "unknown":
        return
    cmd_index.pop(p["id"], None)
    ok = p.get("status") == "done"
    if kind == "in":
        l.update(filled=p.get("filled", 0.0), price=p.get("avg_price"), moves=p.get("moves", l["moves"]))
        if p.get("status") == "error":
            l.update(status="failed", error=p.get("comment"))
        elif l["filled"] > 0:
            l["status"] = "filled"
            await _open_hedge(t, leg.split("_")[0])
        else:
            l["status"] = "canceled"
    elif kind == "out":
        l.update(filled=p.get("filled", 0.0), price=p.get("avg_price"), moves=p.get("moves", l["moves"]))
        i = t["legs"][leg.replace("_out", "_in")]
        if p.get("status") == "error":
            l.update(status="failed", error=p.get("comment"))
        elif ok:
            l["status"] = "filled"
            i.update(status="closed", exit="maker")
        else:
            l["status"] = "canceled"
    elif kind == "force":
        l.update(status="closed" if ok else "failed", error=None if ok else p.get("comment"))
    elif kind == "mt5_open":
        l.update(status="open" if ok else "failed", ticket=p.get("order"), price=p.get("price"), error=None if ok else p.get("comment"))
    elif kind in ("mt5_close", "wmo"):
        if ok or p.get("comment") == "position_not_found":
            l.update(status="closed", exit="wmo" if kind == "wmo" else ("market" if t["forced"] else "market"))
            if kind == "wmo":
                t["wmo_ids"].pop(leg, None)
                if not t["wmo_ids"]:
                    t["wmo"] = "done"
        elif p.get("status") == "canceled":
            pass
        else:
            l.update(status="open", error=p.get("comment"))
    l["updated"] = time.time()
    await _save(t)


# ---- routes

class StartTurn(BaseModel):
    units: float = Field(gt=0)
    leverage: int = Field(ge=1, le=125)
    interval_ms: int = Field(default=500, ge=50, le=60000)


class Wmo(BaseModel):
    on: bool


@router.get("/convert")
async def convert_route(units: float):
    try:
        return await convert(units)
    except (TraderOffline, ResultTimeout) as e:
        raise HTTPException(503, repr(e))


@router.get("/turns/config")
async def turn_config():
    return S()


@router.post("/turns", status_code=201)
async def start_route(body: StartTurn):
    return await start(body.units, body.leverage, body.interval_ms)


@router.get("/turns")
async def list_turns():
    return sorted(turns.values(), key=lambda t: -t["id"])


@router.get("/turns/stream")
async def stream_turns(request: Request):
    async def gen():
        for t in sorted(turns.values(), key=lambda t: t["id"]):
            yield f"event: turn\ndata: {json.dumps(t)}\n\n"
        async for ev, data in bus.subscribe(["turns"]):
            if await request.is_disconnected():
                break
            yield ": ping\n\n" if ev is None else f"event: {ev}\ndata: {json.dumps(data)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@router.get("/turns/{tid}")
async def get_turn(tid: int):
    return _get(tid)


@router.post("/turns/{tid}/close")
async def close_route(tid: int):
    return await close(tid)


@router.post("/turns/{tid}/legs/{leg}/close")
async def close_leg_route(tid: int, leg: str):
    if leg not in LEGS:
        raise HTTPException(404, "unknown leg")
    return await close(tid, {leg})


@router.post("/turns/{tid}/force-close")
async def force_route(tid: int):
    return await force_close(tid)


@router.post("/turns/{tid}/wmo")
async def wmo_route(tid: int, body: Wmo):
    return await wmo(tid, body.on)
