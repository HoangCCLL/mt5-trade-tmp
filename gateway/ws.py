from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from . import bus, config
from .registry import registry, Agent

router = APIRouter()


async def on_trader(a: Agent, msg: dict):
    t, p = msg["type"], msg["payload"]
    if t == "result":
        registry.resolve(a.account_id, p)
        await bus.publish(f"acct:{a.account_id}", "result", p)
    elif t == "ack":
        await bus.publish(f"acct:{a.account_id}", "ack", p)
    elif t == "history":
        await bus.publish(f"acct:{a.account_id}", "history", p)
    elif t == "heartbeat":
        await bus.alive(a.account_id, "trader", p)


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
    if role not in HANDLERS or expected is None or ws.headers.get("authorization") != f"Bearer {expected}":
        await ws.close(code=4401)
        return
    await ws.accept()
    a = registry.get(account_id)
    old = getattr(a, role)
    setattr(a, role, ws)
    if old is not None:
        await old.close(code=4409)  # newest socket wins
    if role == "streamer":
        await registry.resubscribe(a)
    try:
        async for msg in ws.iter_json():
            await HANDLERS[role](a, msg)
    except WebSocketDisconnect:
        pass
    finally:
        if getattr(a, role) is ws:
            setattr(a, role, None)
