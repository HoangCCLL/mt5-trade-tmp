# Gateway (BE) — Spec + FastAPI skeleton

Trạng thái: Draft 1 · Ngày: 2026-10-26 · Đi kèm: `mt5-agent-spec.md`

## 1. Trách nhiệm

Gateway là **một process FastAPI** đứng giữa web app, các agent trên VPS và Redis.

- Giữ WS của mọi agent (`trader`, `streamer`), mỗi account tối đa một socket mỗi role.
- Nhận lệnh từ web app qua HTTP, forward vào đúng `trader`, chờ `result`, trả về.
- Nhận `state/tick/deal/heartbeat` từ `streamer`, ghi Redis, publish.
- Phục vụ browser qua SSE: snapshot từ Redis rồi delta từ pubsub.
- Quản lý refcount subscribe tick theo account + symbol, gửi `subscribe/unsubscribe` xuống `streamer`.

Không làm: xác thực người dùng web app (giả định web app đã auth, gọi gateway bằng API key nội bộ), lưu lịch sử lệnh lâu dài (web app tự lưu từ `result`/`deal`), HA.

## 2. Dữ liệu trong Redis

| Key | Kiểu | Nội dung | TTL |
|---|---|---|---|
| `acct:{id}:state` | hash | `data` = JSON `{account, positions, connected}`, `updated_at` | không |
| `acct:{id}:alive:trader` | string | JSON heartbeat | 15 s |
| `acct:{id}:alive:streamer` | string | JSON heartbeat | 15 s |
| `acct:{id}` | pubsub | `{event, data}` với `event ∈ state, deal, ack, result, history` | |
| `ticks:{id}:{symbol}` | pubsub | `{event:"tick", data:{symbol,bid,ask,time_msc}}` | |

Trạng thái **trong RAM gateway** (mất khi restart, chấp nhận được): registry socket, `pending` future theo command `id`, refcount tick.

## 3. Luồng chính

### 3.1 Đặt lệnh
```
web app ─POST /accounts/A/orders─► gateway
  gateway: id=uuid, future[id], trader_ws.send({id,type:"order.open",ts,payload})
  trader ─ack{id}─► gateway: publish acct:A ack            (viewer thấy "đang chờ broker")
  trader ─result{id,...}─► gateway: future[id].set_result, publish acct:A result
  gateway ─200 result─► web app
timeout 35 s không có result ─► 504 {id}   (web app có thể POST lại cùng id qua header Idempotency-Key)
```

### 3.2 Browser xem
```
browser ─GET /accounts/A/stream?symbols=EURUSD─► gateway
  registry.watch(A,[EURUSD]) → refcount 0→1 → streamer_ws.send(subscribe)
  yield state từ HGET acct:A:state
  SUBSCRIBE acct:A, ticks:A:EURUSD → yield từng event
  mỗi 15 s không có gì → yield ": ping"  (phát hiện client đóng)
browser đóng → generator finally → registry.unwatch → refcount về 0 → chờ 30 s → unsubscribe
```

### 3.3 Agent reconnect
- Socket mới cùng `(account, role)` thắng, socket cũ bị đóng mã 4409.
- `streamer` reconnect: gateway gửi lại `subscribe` cho mọi symbol có refcount > 0.
- `trader` reconnect: `pending` giữ nguyên, `result` về muộn vẫn resolve future nếu chưa timeout.

### 3.4 Gateway restart
- Agent tự reconnect theo backoff. Các HTTP call đang chờ nhận 504. `result` tới sau restart không còn future → chỉ publish; web app reconcile bằng `deal`/`state` hoặc gửi lại cùng `Idempotency-Key`.

## 4. HTTP API

Tất cả route yêu cầu header `X-Api-Key`. Lỗi trả `{"detail": ...}` chuẩn FastAPI.

| Method | Path | Body | Trả |
|---|---|---|---|
| `POST` | `/accounts/{id}/orders` | `OrderOpen` | `result` payload |
| `POST` | `/accounts/{id}/positions/{ticket}/close` | `{volume?}` | `result` |
| `PATCH` | `/accounts/{id}/positions/{ticket}` | `{sl?, tp?}` | `result` |
| `POST` | `/accounts/{id}/history/fill` | `{from?}` | `202 {id}` |
| `GET` | `/accounts/{id}/state` | | snapshot hoặc 404 |
| `GET` | `/accounts/{id}/stream?symbols=a,b` | | SSE |
| `GET` | `/accounts` | | `[{id, trader, streamer, alive}]` |

Header tuỳ chọn `Idempotency-Key` trên các POST lệnh: gateway dùng nó làm command `id` thay vì sinh mới, để web app retry an toàn.

Mã lỗi: `503` trader chưa kết nối · `504` quá 35 s không có `result`, body có `id` · `422` body sai · `401` thiếu API key.

## 5. Cấu trúc code

```
gateway/
  main.py       app, lifespan, include routers           (~20 dòng)
  config.py     env                                      (~10)
  registry.py   Agent, Registry, send_command, watch     (~80)
  ws.py         /agents/{id}/{role}, handler theo role    (~70)
  bus.py        redis: state, alive, publish, subscribe  (~50)
  api.py        HTTP + SSE                               (~90)
```

Phụ thuộc: `fastapi`, `uvicorn[standard]` (đã gồm `websockets`), `redis>=5`. Không thêm gì khác.

Chạy: `uvicorn gateway.main:app --host 0.0.0.0 --port 8000` sau một TLS terminator bất kỳ. Một worker duy nhất, vì registry nằm trong RAM.

## 6. Skeleton

### `config.py`
```python
import os, json

GATEWAY_API_KEY = os.environ["GATEWAY_API_KEY"]
REDIS_URL       = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
AGENT_TOKENS    = json.loads(os.environ["AGENT_TOKENS"])   # {"<account_id>": "<token>"}
RESULT_TIMEOUT  = float(os.environ.get("RESULT_TIMEOUT", 35))
UNSUB_GRACE     = float(os.environ.get("UNSUB_GRACE", 30))
```

### `registry.py`
```python
import asyncio, time, uuid
from fastapi import WebSocket
from . import config

def envelope(type_, payload, id_=None):
    return {"id": id_ or str(uuid.uuid4()), "type": type_, "ts": time.time(), "payload": payload}

class TraderOffline(Exception): ...
class ResultTimeout(Exception):
    def __init__(self, id_): self.id = id_

class Agent:
    def __init__(self, account_id: str):
        self.account_id = account_id
        self.trader: WebSocket | None = None
        self.streamer: WebSocket | None = None
        self.pending: dict[str, asyncio.Future] = {}
        self.tick_refs: dict[str, int] = {}
        self.unsub_timers: dict[str, asyncio.TimerHandle] = {}

class Registry:
    def __init__(self):
        self.agents: dict[str, Agent] = {}

    def get(self, account_id: str) -> Agent:
        return self.agents.setdefault(account_id, Agent(account_id))

    # ---- commands ----
    async def send_command(self, account_id, type_, payload, id_=None) -> dict:
        a = self.get(account_id)
        if a.trader is None:
            raise TraderOffline
        msg = envelope(type_, payload, id_)
        fut = asyncio.get_running_loop().create_future()
        a.pending[msg["id"]] = fut
        try:
            await a.trader.send_json(msg)
            return await asyncio.wait_for(fut, config.RESULT_TIMEOUT)
        except asyncio.TimeoutError:
            raise ResultTimeout(msg["id"])
        finally:
            a.pending.pop(msg["id"], None)

    def resolve(self, account_id, result: dict):
        fut = self.get(account_id).pending.get(result.get("id"))
        if fut and not fut.done():
            fut.set_result(result)

    # ---- tick refcount ----
    async def watch(self, account_id, symbols: list[str]):
        a, fresh = self.get(account_id), []
        for s in symbols:
            if t := a.unsub_timers.pop(s, None):
                t.cancel()
            a.tick_refs[s] = a.tick_refs.get(s, 0) + 1
            if a.tick_refs[s] == 1:
                fresh.append(s)
        if fresh and a.streamer:
            await a.streamer.send_json(envelope("subscribe", {"symbols": fresh}))

    async def unwatch(self, account_id, symbols: list[str]):
        a, loop = self.get(account_id), asyncio.get_running_loop()
        for s in symbols:
            a.tick_refs[s] = max(0, a.tick_refs.get(s, 0) - 1)
            if a.tick_refs[s] == 0:
                a.unsub_timers[s] = loop.call_later(
                    config.UNSUB_GRACE, lambda s=s: asyncio.create_task(self._unsub(a, s)))

    async def _unsub(self, a: Agent, s: str):
        a.unsub_timers.pop(s, None)
        if a.tick_refs.get(s, 0) == 0:
            a.tick_refs.pop(s, None)
            if a.streamer:
                await a.streamer.send_json(envelope("unsubscribe", {"symbols": [s]}))

    async def resubscribe(self, a: Agent):           # streamer vừa reconnect
        syms = [s for s, n in a.tick_refs.items() if n > 0]
        if syms and a.streamer:
            await a.streamer.send_json(envelope("subscribe", {"symbols": syms}))

registry = Registry()
```

### `bus.py`
```python
import asyncio, json, time
import redis.asyncio as redis
from . import config

r = redis.from_url(config.REDIS_URL, decode_responses=True)

async def save_state(acct, state):
    await r.hset(f"acct:{acct}:state", mapping={"data": json.dumps(state), "updated_at": time.time()})

async def get_state(acct):
    raw = await r.hget(f"acct:{acct}:state", "data")
    return json.loads(raw) if raw else None

async def alive(acct, role, hb):
    await r.set(f"acct:{acct}:alive:{role}", json.dumps(hb), ex=15)

async def is_alive(acct, role) -> bool:
    return bool(await r.exists(f"acct:{acct}:alive:{role}"))

async def publish(channel, event, data):
    await r.publish(channel, json.dumps({"event": event, "data": data}))

async def subscribe(channels: list[str], idle=15.0):
    """Yield (event, data); yield (None, None) khi idle quá `idle` giây để caller gửi ping."""
    ps = r.pubsub()
    await ps.subscribe(*channels)
    try:
        while True:
            try:
                msg = await asyncio.wait_for(ps.get_message(ignore_subscribe_messages=True, timeout=idle), idle + 1)
            except asyncio.TimeoutError:
                msg = None
            if msg is None:
                yield None, None
                continue
            body = json.loads(msg["data"])
            yield body["event"], body["data"]
    finally:
        await ps.unsubscribe(*channels)
        await ps.aclose()
```

### `ws.py`
```python
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
    if role not in HANDLERS or ws.headers.get("authorization") != f"Bearer {config.AGENT_TOKENS.get(account_id)}":
        await ws.close(code=4401); return
    await ws.accept()
    a = registry.get(account_id)
    old = getattr(a, role)
    setattr(a, role, ws)
    if old is not None:
        await old.close(code=4409)                 # socket mới thắng
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
```

### `api.py`
```python
import json
from typing import Literal
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
from . import bus, config
from .registry import registry, TraderOffline, ResultTimeout

def api_key(x_api_key: str = Header()):
    if x_api_key != config.GATEWAY_API_KEY:
        raise HTTPException(401)

router = APIRouter(prefix="/accounts", dependencies=[Depends(api_key)])

class OrderOpen(BaseModel):
    symbol: str
    side: Literal["buy", "sell"]
    volume: float = Field(gt=0)
    sl: float | None = None
    tp: float | None = None
    magic: int = Field(ge=0)
    comment: str | None = Field(default=None, max_length=31)
    deviation: int = 20

class PositionClose(BaseModel):
    volume: float | None = Field(default=None, gt=0)

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

@router.post("/{acct}/orders")
async def open_order(acct: str, body: OrderOpen, idempotency_key: str | None = Header(default=None)):
    return await run(acct, "order.open", body.model_dump(exclude_none=True), idempotency_key)

@router.post("/{acct}/positions/{ticket}/close")
async def close_position(acct: str, ticket: int, body: PositionClose, idempotency_key: str | None = Header(default=None)):
    return await run(acct, "order.close", {"position": ticket, **body.model_dump(exclude_none=True)}, idempotency_key)

@router.patch("/{acct}/positions/{ticket}")
async def modify_position(acct: str, ticket: int, body: PositionModify, idempotency_key: str | None = Header(default=None)):
    return await run(acct, "order.modify", {"position": ticket, **body.model_dump(exclude_none=True)}, idempotency_key)

@router.post("/{acct}/history/fill", status_code=202)
async def history_fill(acct: str, body: HistoryFill):
    a = registry.get(acct)
    if a.trader is None:
        raise HTTPException(503, "trader offline")
    from .registry import envelope
    msg = envelope("history.fill", body.model_dump(exclude_none=True, by_alias=True))
    await a.trader.send_json(msg)                  # không chờ result, dữ liệu về qua streamer
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
            if (s := await bus.get_state(acct)) is not None:
                yield sse("state", s)
            chans = [f"acct:{acct}", *[f"ticks:{acct}:{s}" for s in syms]]
            async for event, data in bus.subscribe(chans):
                if event is None:
                    yield ": ping\n\n"             # giữ kết nối, phát hiện client đóng
                else:
                    yield sse(event, data)
        finally:
            await registry.unwatch(acct, syms)
    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})
```

### `main.py`
```python
from contextlib import asynccontextmanager
from fastapi import FastAPI
from . import api, ws, bus

@asynccontextmanager
async def lifespan(app):
    await bus.r.ping()
    yield
    await bus.r.aclose()

app = FastAPI(title="MT5 Gateway", lifespan=lifespan)
app.include_router(ws.router)
app.include_router(api.router)
```

## 7. Ghi chú thiết kế

- **Một worker uvicorn.** Registry và `pending` nằm trong RAM. Nhiều worker = agent nối worker A, HTTP vào worker B, không tìm thấy socket. Khi cần HA mới chuyển routing command qua Redis (phase 2).
- **SSE thay vì WS cho browser.** Chiều xuống một chiều, lệnh đi HTTP. Browser dùng `EventSource`, tự reconnect, không cần lib.
- **`: ping` 15 s** vừa giữ proxy không cắt kết nối, vừa là cách duy nhất để `StreamingResponse` biết client đã đóng (generator chỉ nhận lỗi khi cố `yield`).
- **`history.fill` không chờ `result`** vì dữ liệu về theo batch qua event `history` (trader gửi) và có thể mất phút. Web app theo dõi `history` trên SSE hoặc pubsub.
- **Không có DB.** Web app đã có DB riêng, tự lưu `result`/`deal` từ response hoặc pubsub. Gateway chỉ là relay có trạng thái gần nhất.
- **Pydantic validate một lần ở gateway.** Agent vẫn tự validate payload (hai bên không tin nhau), nhưng lỗi 422 được chặn trước khi tốn một round-trip tới VPS.

## 8. Tiêu chí hoàn thành MVP gateway

1. Agent giả (script Python nối WS, trả `ack`+`result` sau 2 s): `POST /orders` trả `result` trong 2.0 ± 0.05 s.
2. Agent giả trả `result` sau 40 s: HTTP nhận 504 với `id`, `result` về muộn vẫn được publish lên `acct:{id}`.
3. Hai `EventSource` cùng mở với cùng symbol: `streamer` nhận đúng một `subscribe`. Đóng cả hai: `unsubscribe` tới sau 30 s, không sớm hơn.
4. Kill `streamer` rồi nối lại: nhận `subscribe` chứa đủ symbol đang xem.
5. Nối hai `trader` cùng account: socket cũ nhận close 4409, lệnh đi vào socket mới.


## Observability: latency breakdown per lệnh

Mỗi `result` của `order.*` được gateway gắn thêm `timing` (ms), tính từ các mốc đã có sẵn, không chạm vào đường thực thi lệnh:

| hop | từ → đến | đồng hồ | ý nghĩa |
|---|---|---|---|
| `ack` | gateway gửi lệnh → nhận `ack` | gateway | round trip mạng + loop agent, không phụ thuộc skew |
| `down` | gateway gửi → agent nhận (`t_recv`) | agent + skew | mạng chiều xuống |
| `pool` | `d_pool` (perf_counter) | agent | chờ thread trong `TRADE_POOL`; tăng khi nhiều lệnh đồng thời |
| `mt5` | `d_mt5` (perf_counter) | agent | `order_check` + `order_send` tại broker; phần không kiểm soát được |
| `up` | agent gửi result (`t_sent`) → gateway nhận | agent + skew | mạng chiều lên |
| `total` | HTTP vào → result về | gateway | cái client cảm nhận |

Agent chỉ thêm hai trường vào result: `t_skew` (skew đo từ `hello`) và `t_sent`. Gateway lưu 500 timing gần nhất vào Redis list `metrics:{acct}`; `GET /accounts/{acct}/metrics?n=100` trả `recent` và `p50/p95/max` từng hop. UI có bảng Latency. Nếu `down`/`up` âm hoặc lệch bất thường trong khi `ack` ổn, nghi skew đo sai (VPS đổi giờ), không phải mạng.
