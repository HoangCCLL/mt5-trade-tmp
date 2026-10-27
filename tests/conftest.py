import asyncio, json, os, socket

os.environ.update(
    GATEWAY_API_KEY="k",
    AGENT_TOKENS=json.dumps({"A1": "tokA1", "A2": "tokA2"}),
    REDIS_URL="redis://localhost:6379/15",
    RESULT_TIMEOUT="1",
    UNSUB_GRACE="0.5",
)

import httpx, pytest, uvicorn, websockets
from websockets.asyncio.client import connect

API = {"X-Api-Key": "k"}


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture
async def server():
    from gateway import bus
    from gateway.main import app
    from gateway.registry import registry

    registry.agents.clear()
    await bus.r.flushdb()
    port = free_port()
    srv = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning"))
    task = asyncio.create_task(srv.serve())
    while not srv.started:
        await asyncio.sleep(0.02)
    yield f"http://127.0.0.1:{port}"
    srv.should_exit = True
    await task


@pytest.fixture
async def http(server):
    async with httpx.AsyncClient(base_url=server, headers=API, timeout=10) as c:
        yield c


class FakeAgent:
    """Test double speaking the agent WS protocol. Records every message received."""

    def __init__(self, base, acct, role, token=None, result_delay=0.0, result=None):
        self.url = base.replace("http", "ws") + f"/agents/{acct}/{role}"
        self.token = token or f"tok{acct}"
        self.role, self.result_delay = role, result_delay
        self.result = result or {"status": "done", "retcode": 10009, "order": 1, "deal": 2}
        self.received: list[dict] = []
        self.hellos = 0
        self.ws = None
        self._task = None

    async def __aenter__(self):
        self.ws = await connect(self.url, additional_headers={"Authorization": f"Bearer {self.token}"})
        self._task = asyncio.create_task(self._loop())
        return self

    async def __aexit__(self, *_):
        self._task.cancel()
        await self.ws.close()

    async def _loop(self):
        try:
            async for raw in self.ws:
                msg = json.loads(raw)
                if msg["type"] == "hello":  # clock sync, not part of the command flow under test
                    self.hellos += 1
                    continue
                self.received.append(msg)
                if self.role == "trader" and msg["type"].startswith("order."):
                    asyncio.create_task(self._reply(msg["id"]))
        except websockets.ConnectionClosed:
            pass

    async def _reply(self, cid):
        await self.send("ack", {"id": cid})
        await asyncio.sleep(self.result_delay)
        await self.send("result", {"id": cid, **self.result})

    async def send(self, type_, payload):
        await self.ws.send(json.dumps({"id": "x", "type": type_, "ts": 0, "payload": payload}))

    async def wait_for(self, type_, timeout=3.0):
        async with asyncio.timeout(timeout):
            while True:
                for m in self.received:
                    if m["type"] == type_:
                        return m
                await asyncio.sleep(0.02)

    @property
    def closed(self):
        return self.ws.state.name == "CLOSED"
