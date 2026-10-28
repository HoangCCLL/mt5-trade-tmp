import hmac
import time
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from . import bus, config
from .registry import registry, Agent, envelope
from . import turns

router = APIRouter()


async def on_trader(a: Agent, msg: dict):
    t, p = msg["type"], msg["payload"]
    if t == "result":
        registry.resolve(a.account_id, p)
        await turns.on_event(a.account_id, "result", p)
        if p.get("id") and p.get("status") != "unknown":
            await bus.sched_del(a.account_id, p["id"])   # a scheduled exit that finished (done/error/canceled)
        if p.get("timing", {}).get("cmd") in ("order.open", "order.close", "order.modify"):  # latency stats: single orders only
            await bus.record_timing(a.account_id, {"id": p["id"], "status": p.get("status"), "at": time.time(), **p["timing"]})
        await bus.publish(f"acct:{a.account_id}", "result", p)
    elif t == "update":   # progress of a long-running command (chase moves, scheduled exits)
        await bus.publish(f"acct:{a.account_id}", "update", p)
        await turns.on_event(a.account_id, "update", p)
    elif t == "ack":
        registry.acked(a.account_id, p.get("id"))
        await bus.publish(f"acct:{a.account_id}", "ack", p)
    elif t == "history":
        await bus.publish(f"acct:{a.account_id}", "history", p)
    elif t == "heartbeat":
        await bus.alive(a.account_id, "trader", p)
        await a.trader.send_json(envelope("hello", {"ts": time.time(), "echo": p.get("t")}))  # agent derives RTT-corrected skew


async def on_streamer(a: Agent, msg: dict):
    t, p = msg["type"], msg["payload"]
    if t == "state":
        await bus.save_state(a.account_id, p)
        await bus.publish(f"acct:{a.account_id}", "state", p)
    elif t == "tick":
        await bus.publish(f"ticks:{a.account_id}:{p['symbol']}", "tick", p)
    elif t in ("deal", "history"):
        await bus.publish(f"acct:{a.account_id}", t, p)
    elif t == "heartbeat":
        await bus.alive(a.account_id, "streamer", p)


HANDLERS = {"trader": on_trader, "streamer": on_streamer}


@router.websocket("/agents/{account_id}/{role}")
async def agent_ws(ws: WebSocket, account_id: str, role: str):
    expected = config.AGENT_TOKENS.get(account_id)
    if role not in HANDLERS or expected is None or not hmac.compare_digest(ws.headers.get("authorization", ""), f"Bearer {expected}"):
        await ws.close(code=4401)
        return
    await ws.accept()
    await ws.send_json(envelope("hello", {"ts": time.time()}))  # agent derives its clock skew from this
    a = registry.get(account_id)
    old = getattr(a, role)
    setattr(a, role, ws)
    if old is not None:
        await old.close(code=4409)  # newest socket wins
    if role == "streamer":
        await registry.resubscribe(a)
    if role == "trader":   # re-arm scheduled exits the agent may have lost (VPS reboot over the weekend)
        for sid, payload in (await bus.sched_all(account_id)).items():
            await ws.send_json(envelope("order.close", payload, sid))
        await turns.resend(account_id)   # and chases that were running on it
    try:
        async for msg in ws.iter_json():
            await HANDLERS[role](a, msg)
    except WebSocketDisconnect:
        pass
    finally:
        if getattr(a, role) is ws:
            setattr(a, role, None)
