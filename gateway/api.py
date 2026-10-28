import hmac
import json, uuid
from typing import Literal
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from . import bus, config
from .registry import registry, envelope, TraderOffline, ResultTimeout


def api_key(x_api_key: str = Header()):
    if not hmac.compare_digest(x_api_key or "", config.GATEWAY_API_KEY):
        raise HTTPException(401)


router = APIRouter(prefix="/accounts", dependencies=[Depends(api_key)])


class OrderOpen(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    volume: float = Field(gt=0)
    type: Literal["market", "limit"] = "market"   # limit: Binance resting order (MT5 agent ignores it for now)
    price: float | None = None
    reduce_only: bool | None = None
    sl: float | None = None
    tp: float | None = None
    magic: int = Field(ge=0)
    comment: str | None = Field(default=None, max_length=31)
    deviation: int = 20


class PositionClose(BaseModel):
    volume: float | None = Field(default=None, gt=0)
    when: Literal["market_open"] | None = None   # scheduled exit at the next weekly open (MT5); see docs/trade-turn-spec.md
    max_spread_points: int | None = Field(default=None, ge=0)
    deviation: int | None = Field(default=None, ge=0)


class ChaseOrder(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    volume: float = Field(gt=0)
    reduce_only: bool = False
    interval_ms: int = Field(default=500, ge=50, le=60000)
    magic: int = Field(default=0, ge=0)
    turn: int | None = None
    leg: str | None = None


class Leverage(BaseModel):
    symbol: str
    leverage: int = Field(ge=1, le=125)


class OrderCancel(BaseModel):
    symbol: str
    order: int


class PositionModify(BaseModel):
    sl: float | None = None
    tp: float | None = None


class HistoryFill(BaseModel):
    from_: float | None = Field(default=None, alias="from")


async def run(acct, type_, payload, idem: str | None):
    try:
        return await registry.send_command(acct, type_, payload, id_=idem)
    except TraderOffline:
        raise HTTPException(503, "trader offline")
    except ResultTimeout as e:
        raise HTTPException(504, {"id": e.id, "detail": "no result within timeout"})


@router.get("/{acct}/symbols")
async def symbols(acct: str):
    """Market Watch symbols of the terminal, via the trader (one IPC call, ~ms)."""
    r = await run(acct, "symbols.list", {}, None)
    return {"symbols": r.get("symbols", [])}


@router.post("/{acct}/orders")
async def open_order(acct: str, body: OrderOpen, idempotency_key: str | None = Header(default=None)):
    return await run(acct, "order.open", body.model_dump(exclude_none=True), idempotency_key)


async def start(acct, type_, payload):
    """Long-running command: 202 with the command id; the result comes back on the account stream."""
    try:
        return JSONResponse({"id": await registry.send_command(acct, type_, payload, wait=False)}, status_code=202)
    except TraderOffline:
        raise HTTPException(503, "trader offline")


@router.post("/{acct}/orders/chase")
async def chase_order(acct: str, body: ChaseOrder):
    """Binance: post-only limit at the queue, moved with the best price until filled or canceled (see docs/trade-turn-spec.md)."""
    return await start(acct, "order.chase", body.model_dump(exclude_none=True))


@router.post("/{acct}/commands/{cmd_id}/cancel")
async def cancel_command(acct: str, cmd_id: str):
    return await run(acct, "chase.cancel", {"id": cmd_id}, None)


@router.post("/{acct}/leverage")
async def leverage(acct: str, body: Leverage):
    return await run(acct, "account.leverage", body.model_dump(), None)


@router.get("/{acct}/symbols/{symbol}")
async def symbol_spec(acct: str, symbol: str):
    r = await run(acct, "symbol.spec", {"symbol": symbol}, None)
    if r.get("status") != "done":
        raise HTTPException(404, r.get("comment", "unknown symbol"))
    return r["spec"]


@router.post("/{acct}/orders/cancel")
async def cancel_order(acct: str, body: OrderCancel, idempotency_key: str | None = Header(default=None)):
    """Cancel a resting order (Binance). MT5 pending orders are not supported yet."""
    return await run(acct, "order.cancel", body.model_dump(), idempotency_key)


@router.post("/{acct}/positions/{ticket}/close")
async def close_position(acct: str, ticket: str, body: PositionClose | None = None,
                         idempotency_key: str | None = Header(default=None)):
    extra = body.model_dump(exclude_none=True) if body else {}
    pos = int(ticket) if ticket.isdigit() else ticket   # MT5 tickets are ints, Binance positions are symbols
    if extra.get("when"):
        sid = idempotency_key or str(uuid.uuid4())
        payload = {"position": pos, **extra}
        await bus.sched_put(acct, sid, payload)
        try:
            await registry.send_command(acct, "order.close", payload, id_=sid, wait=False)
        except TraderOffline:
            pass   # armed on (re)connect
        return JSONResponse({"id": sid}, status_code=202)
    return await run(acct, "order.close", {"position": pos, **extra}, idempotency_key)


@router.get("/{acct}/scheduled")
async def scheduled(acct: str):
    return [{"id": k, **v} for k, v in (await bus.sched_all(acct)).items()]


@router.delete("/{acct}/scheduled/{sid}")
async def unschedule(acct: str, sid: str):
    await bus.sched_del(acct, sid)
    try:
        r = await run(acct, "schedule.cancel", {"id": sid}, None)
        return {"canceled": bool(r.get("canceled"))}
    except HTTPException:
        return {"canceled": True}   # trader offline: it will not be re-armed, nothing to cancel there


@router.patch("/{acct}/positions/{ticket}")
async def modify_position(acct: str, ticket: str, body: PositionModify,
                          idempotency_key: str | None = Header(default=None)):
    pos = int(ticket) if ticket.isdigit() else ticket
    return await run(acct, "order.modify", {"position": pos, **body.model_dump(exclude_none=True)}, idempotency_key)


@router.post("/{acct}/history/fill", status_code=202)
async def history_fill(acct: str, body: HistoryFill | None = None):
    a = registry.get(acct)
    if a.trader is None:
        raise HTTPException(503, "trader offline")
    msg = envelope("history.fill", body.model_dump(exclude_none=True, by_alias=True) if body else {})
    await a.trader.send_json(msg)  # no result awaited: data arrives via streamer `history` events
    return {"id": msg["id"]}


@router.get("/{acct}/state")
async def state(acct: str):
    s = await bus.get_state(acct)
    if s is None:
        raise HTTPException(404)
    return s


@router.get("")
async def accounts():
    return [{"id": a.account_id, "trader": a.trader is not None, "streamer": a.streamer is not None,
             "alive": await bus.is_alive(a.account_id, "streamer")} for a in registry.agents.values()]


def sse(event, data):
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"


@router.get("/{acct}/stream")
async def stream(acct: str, request: Request, symbols: str = ""):
    syms = [s for s in symbols.split(",") if s]

    async def gen():
        await registry.watch(acct, syms)
        try:
            s = await bus.get_state(acct)
            yield sse("state", s) if s is not None else ": hello\n\n"
            chans = [f"acct:{acct}", *[f"ticks:{acct}:{s}" for s in syms]]
            async for event, data in bus.subscribe(chans):
                yield ": ping\n\n" if event is None else sse(event, data)  # ping doubles as disconnect probe
        finally:
            await registry.unwatch(acct, syms)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


SEGMENTS = ("ack", "down", "pool", "mt5", "up", "total")


def _agg(vals):
    vals = sorted(v for v in vals if v is not None)
    if not vals:
        return None
    q = lambda p: vals[min(len(vals) - 1, round(p * (len(vals) - 1)))]  # noqa: E731
    return {"p50": q(0.5), "p95": q(0.95), "max": vals[-1], "n": len(vals)}


@router.get("/{acct}/metrics")
async def metrics(acct: str, n: int = 100):
    """Latency breakdown (ms) of the last n commands: where the time went, per hop."""
    recent = await bus.recent_timings(acct, n)
    return {"count": len(recent), "recent": recent, **{k: _agg(t.get(k) for t in recent) for k in SEGMENTS}}
