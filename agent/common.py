import asyncio, json, logging, os, time, uuid
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed
from .mt5_api import mt5

log = logging.getLogger("agent")
FATAL_CLOSE_CODES = {4401, 4409}  # bad token / replaced by a newer socket: do not fight, exit


def envelope(type_, payload):
    return {"id": str(uuid.uuid4()), "type": type_, "ts": time.time(), "payload": payload}


def init_mt5():
    kw = {}
    if os.environ.get("MT5_PATH"):
        kw["path"] = os.environ["MT5_PATH"]
    if os.environ.get("MT5_LOGIN"):
        kw.update(login=int(os.environ["MT5_LOGIN"]), password=os.environ["MT5_PASSWORD"],
                  server=os.environ["MT5_SERVER"])
    if not mt5.initialize(**kw):
        raise SystemExit(f"mt5.initialize failed: {mt5.last_error()}")


class Link:
    """Persistent outbound WS to the gateway. Outbox survives reconnects; inbound messages become tasks."""

    def __init__(self, url, token):
        self.url, self.token = url, token
        self.outbox: asyncio.Queue = asyncio.Queue()

    def emit(self, type_, payload):
        self.outbox.put_nowait(envelope(type_, payload))

    async def run(self, on_message):
        backoff = 1
        while True:
            try:
                async with connect(self.url, additional_headers={"Authorization": f"Bearer {self.token}"},
                                   ping_interval=5, ping_timeout=10) as ws:
                    log.info("connected %s", self.url)
                    backoff = 1
                    sender = asyncio.create_task(self._sender(ws))
                    try:
                        async for raw in ws:
                            asyncio.create_task(self._dispatch(on_message, json.loads(raw)))
                    finally:
                        sender.cancel()
            except ConnectionClosed as e:
                if e.rcvd and e.rcvd.code in FATAL_CLOSE_CODES:
                    log.error("gateway closed with %s, exiting", e.rcvd.code)
                    return
                log.warning("ws closed: %s", e)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("ws error: %r", e)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)

    async def _sender(self, ws):
        while True:
            msg = await self.outbox.get()
            try:
                await ws.send(json.dumps(msg))
            except Exception:
                self.outbox.put_nowait(msg)
                raise

    @staticmethod
    async def _dispatch(on_message, msg):
        try:
            await on_message(msg)
        except Exception:
            log.exception("handler failed for %s", msg.get("type"))


async def heartbeat(emit, extra, period=5.0):
    while True:
        emit("heartbeat", {"connected": bool(mt5.terminal_info() and mt5.terminal_info().connected), **extra()})
        await asyncio.sleep(period)
