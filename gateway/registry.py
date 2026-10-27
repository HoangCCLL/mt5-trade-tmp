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
        self.timing: dict[str, dict] = {}  # gateway-clock timestamps per in-flight command
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
        tm = a.timing[msg["id"]] = {"t_http": time.time(), "cmd": type_}
        try:
            await a.trader.send_json(msg)
            tm["t_sent"] = time.time()
            return await asyncio.wait_for(fut, config.RESULT_TIMEOUT)
        except asyncio.TimeoutError:
            raise ResultTimeout(msg["id"])
        finally:
            a.pending.pop(msg["id"], None)
            a.timing.pop(msg["id"], None)

    def acked(self, account_id, id_):
        tm = self.get(account_id).timing.get(id_)
        if tm:
            tm.setdefault("t_ack", time.time())

    def resolve(self, account_id, result: dict):
        a = self.get(account_id)
        tm = a.timing.get(result.get("id"))
        if tm:
            tm["t_result"] = time.time()
            result["timing"] = latency_breakdown(tm, result)
        fut = a.pending.get(result.get("id"))
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


def latency_breakdown(tm: dict, res: dict) -> dict:
    """Milliseconds per hop. Gateway timestamps (tm) are gateway clock; agent timestamps (res) are agent clock
    shifted by the skew the agent measured from `hello`. `ack` and `total` use the gateway clock only.
    A hop that is negative or longer than `total` is physically impossible (clock skew not known, old agent),
    so it is reported as None rather than as a misleading number."""
    skew = res.get("t_skew") or 0.0
    ag = lambda k: None if res.get(k) is None else res[k] + skew  # noqa: E731
    ms = lambda a, b: None if a is None or b is None else round((a - b) * 1000, 1)  # noqa: E731
    total = ms(tm.get("t_result"), tm.get("t_http"))
    hops = {
        "ack": ms(tm.get("t_ack"), tm.get("t_sent")),            # gateway -> agent -> gateway, pure gateway clock
        "down": ms(ag("t_recv"), tm.get("t_sent")),              # gateway -> agent
        # intra-agent durations: prefer the agent's perf_counter values (wall clock on Windows ticks at 15.6 ms)
        "pool": round(res["d_pool"] * 1000, 1) if res.get("d_pool") is not None else ms(ag("t_mt5_start"), ag("t_recv")),
        "mt5": round(res["d_mt5"] * 1000, 1) if res.get("d_mt5") is not None else ms(ag("t_mt5_end"), ag("t_mt5_start")),
        "up": ms(tm.get("t_result"), ag("t_sent") or ag("t_mt5_end")),  # agent -> gateway
    }
    sane = lambda v: None if v is None or total is None or v < 0 or v > total else v  # noqa: E731
    return {"cmd": tm.get("cmd"), **{k: sane(v) for k, v in hops.items()}, "total": total}
