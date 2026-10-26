import asyncio, time, uuid
from fastapi import WebSocket
from . import config


def envelope(type_, payload, id_=None):
    return {"id": id_ or str(uuid.uuid4()), "type": type_, "ts": time.time(), "payload": payload}


class TraderOffline(Exception):
    pass


class ResultTimeout(Exception):
    def __init__(self, id_):
        self.id = id_


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
            if a.tick_refs[s] == 0 and s not in a.unsub_timers:
                a.unsub_timers[s] = loop.call_later(
                    config.UNSUB_GRACE, lambda s=s: asyncio.create_task(self._unsub(a, s)))

    async def _unsub(self, a: Agent, s: str):
        a.unsub_timers.pop(s, None)
        if a.tick_refs.get(s, 0) == 0:
            a.tick_refs.pop(s, None)
            if a.streamer:
                await a.streamer.send_json(envelope("unsubscribe", {"symbols": [s]}))

    async def resubscribe(self, a: Agent):
        syms = [s for s, n in a.tick_refs.items() if n > 0]
        if syms and a.streamer:
            await a.streamer.send_json(envelope("subscribe", {"symbols": syms}))


registry = Registry()
