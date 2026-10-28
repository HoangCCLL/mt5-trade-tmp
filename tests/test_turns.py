"""Trade turn orchestration: two Binance accounts (fake exchanges) + one MT5 account (fake terminal), real gateway."""
import asyncio, os
import pytest

os.environ["MT5_FAKE"] = "1"


class Lab:
    """Three agents in-process; `restart(acct)` simulates an agent process restart (exchange state survives)."""

    def __init__(self, base):
        from binance.fake import Fake
        self.base, self.exl, self.exs, self.tasks = base, Fake(), Fake(), {}
        for ex in (self.exl, self.exs):
            ex.prices["XAUUSDT"] = (4000.0, 4000.5)

    def _run(self, acct):
        from binance import trader as btrader
        from agent import trader as mtrader
        if acct == "A3":
            return asyncio.create_task(mtrader.run(self.base + "/agents/A3/trader", "tokA3", order_timeout=1.0, ttl=5.0))
        ex = self.exl if acct == "A1" else self.exs
        return asyncio.create_task(btrader.run(self.base + f"/agents/{acct}/trader", f"tok{acct}", ex, order_timeout=1.0, ttl=5.0))

    async def start(self):
        for acct in ("A1", "A2", "A3"):
            self.tasks[acct] = self._run(acct)
        await asyncio.sleep(0.4)

    async def restart(self, acct):
        from binance import trader as btrader
        self.tasks[acct].cancel()
        btrader.chases.clear()   # a new process has no running chases
        await asyncio.sleep(0.1)
        self.tasks[acct] = self._run(acct)
        await asyncio.sleep(0.4)

    def __iter__(self):
        from agent.mt5_api import mt5
        return iter((self.exl, self.exs, mt5))


@pytest.fixture
async def turn_agents(server):
    from agent.mt5_api import mt5
    mt5.reset()
    lab = Lab(server.replace("http", "ws"))
    await lab.start()
    yield lab
    for t in lab.tasks.values():
        t.cancel()


async def wait_turn(http, tid, cond, timeout=3.0):
    t = None
    try:
        async with asyncio.timeout(timeout):
            while True:
                t = (await http.get(f"/turns/{tid}")).json()
                if cond(t):
                    return t
                await asyncio.sleep(0.05)
    except TimeoutError:
        raise AssertionError(f"turn never reached condition; last: status={t and t.get('status')} legs={t and {k: (v['status'], v.get('error'), v.get('price')) for k, v in t['legs'].items()}}")


async def test_convert_preview_uses_both_specs(server, http, turn_agents):
    r = (await http.get("/convert?units=5")).json()
    assert r["binance"] == 5 and r["mt5"] == 0.05 and r["base"] == "XAU" and r["ok"] is True
    r = (await http.get("/convert?units=0.3")).json()
    assert r["ok"] is False and "mt5" in r["reason"]


async def test_turn_opens_mt5_hedges_as_binance_chases_fill(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    r = await http.post("/turns", json={"units": 5, "leverage": 10})
    assert r.status_code == 201, r.text
    t = r.json(); tid = t["id"]
    assert t["volume"] == {"binance": 5, "mt5": 0.05} and t["status"] == "opening"
    from gateway import turns
    assert t["interval_ms"] == turns.CHASE_INTERVAL_MS   # fixed in code for the MVP, not a form field
    assert exl.leverage["XAUUSDT"] == 10 and exs.leverage["XAUUSDT"] == 10
    t = await wait_turn(http, tid, lambda t: t["legs"]["long_in"]["status"] == "chasing" and t["legs"]["short_in"]["status"] == "chasing")
    assert t["legs"]["long_in"]["price"] == 4000.0 and t["legs"]["short_in"]["price"] == 4000.5   # queue on each side
    exl.hit_resting()                                                   # long filled -> MT5 sell
    t = await wait_turn(http, tid, lambda t: t["legs"]["mt5_short"]["status"] == "open")
    assert t["legs"]["long_in"]["status"] == "filled" and t["legs"]["long_in"]["filled"] == 5
    pos = mt5.positions_get()
    assert len(pos) == 1 and pos[0].type == 1 and pos[0].volume == 0.05 and pos[0].magic == tid and pos[0].ticket == t["legs"]["mt5_short"]["ticket"]
    exs.hit_resting()                                                   # short filled -> MT5 buy
    t = await wait_turn(http, tid, lambda t: t["status"] == "open")
    assert t["legs"]["mt5_long"]["status"] == "open" and len(mt5.positions_get()) == 2
    assert [x["id"] for x in (await http.get("/turns")).json()] == [tid]


async def open_turn(http, exl, exs):
    tid = (await http.post("/turns", json={"units": 5, "leverage": 10})).json()["id"]
    await wait_turn(http, tid, lambda t: t["legs"]["short_in"]["status"] == "chasing")
    exl.hit_resting(); exs.hit_resting()
    return await wait_turn(http, tid, lambda t: t["status"] == "open")


async def test_close_turn_chases_out_and_closes_mt5(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    t = await open_turn(http, exl, exs)
    assert (await http.post(f"/turns/{t['id']}/close")).status_code == 200
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_out"]["status"] == "chasing" and t["legs"]["mt5_long"]["status"] == "closed")
    assert t["status"] == "closing" and mt5.positions_get() == ()
    out = [o for o in exl.orders.values() if o["status"] == "NEW"][0]
    assert out["side"] == "SELL" and out["reduceOnly"] and out["origQty"] == 5
    exl.hit_resting(); exs.hit_resting()
    t = await wait_turn(http, t["id"], lambda t: t["status"] == "closed")
    assert exl.pos == {} and exs.pos == {} and t["legs"]["short_out"]["filled"] == 5


async def test_force_close_cancels_chases_and_markets_out(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    tid = (await http.post("/turns", json={"units": 5, "leverage": 10})).json()["id"]
    await wait_turn(http, tid, lambda t: t["legs"]["short_in"]["status"] == "chasing")
    exl.hit_resting()                                                   # only the long side got in
    await wait_turn(http, tid, lambda t: t["legs"]["mt5_short"]["status"] == "open")
    assert (await http.post(f"/turns/{tid}/force-close")).status_code == 200
    t = await wait_turn(http, tid, lambda t: t["status"] == "force_closed" and t["legs"]["long_in"]["status"] == "closed")
    assert t["legs"]["short_in"]["status"] == "canceled" and t["legs"]["mt5_short"]["status"] == "closed"
    assert exl.pos == {} and not [o for o in exs.orders.values() if o["status"] == "NEW"] and mt5.positions_get() == ()
    assert t["legs"]["long_in"]["exit"] == "market"


async def test_wmo_only_when_binance_is_out_and_mt5_still_in(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    t = await open_turn(http, exl, exs)
    assert (await http.post(f"/turns/{t['id']}/wmo", json={"on": True})).status_code == 409
    for leg in ("long_in", "short_in"):
        assert (await http.post(f"/turns/{t['id']}/legs/{leg}/close")).status_code == 200
    await wait_turn(http, t["id"], lambda t: t["legs"]["short_out"]["status"] == "chasing")
    exl.hit_resting(); exs.hit_resting()
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_out"]["status"] == "filled" and t["legs"]["short_out"]["status"] == "filled")
    assert t["legs"]["mt5_short"]["status"] == "open" and t["wmo"] == "off"
    assert (await http.post(f"/turns/{t['id']}/wmo", json={"on": True})).status_code == 200
    t = await wait_turn(http, t["id"], lambda t: t["wmo"] == "armed")
    assert len((await http.get("/accounts/A3/scheduled")).json()) == 2
    assert (await http.post(f"/turns/{t['id']}/wmo", json={"on": False})).status_code == 200
    await wait_turn(http, t["id"], lambda t: t["wmo"] == "off")
    assert (await http.get("/accounts/A3/scheduled")).json() == []


async def test_turn_limit_is_twenty(server, http, turn_agents, monkeypatch):
    from gateway import turns
    monkeypatch.setattr(turns, "MAX_OPEN", 1)
    assert (await http.post("/turns", json={"units": 5, "leverage": 10})).status_code == 201
    assert (await http.post("/turns", json={"units": 5, "leverage": 10})).status_code == 409
    assert (await http.post("/turns", json={"units": 5, "leverage": 99})).status_code == 422


async def test_closing_a_chasing_out_leg_cancels_it_and_can_be_restarted(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    t = await open_turn(http, exl, exs)
    await http.post(f"/turns/{t['id']}/legs/long_in/close")
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_out"]["status"] == "chasing")
    await http.post(f"/turns/{t['id']}/legs/long_out/close")          # cancel the exit chase, keep the position
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_out"]["status"] == "canceled")
    assert t["legs"]["long_in"]["status"] == "filled" and exl.pos["XAUUSDT"]["amt"] == 5
    await http.post(f"/turns/{t['id']}/legs/long_in/close")          # start a new exit chase
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_out"]["status"] == "chasing")
    exl.hit_resting()
    t = await wait_turn(http, t["id"], lambda t: t["legs"]["long_in"]["status"] == "closed")
    assert exl.pos == {}


async def test_chase_resumes_with_remaining_qty_after_agent_restart(server, http, turn_agents):
    exl, exs, mt5 = turn_agents
    tid = (await http.post("/turns", json={"units": 5, "leverage": 10})).json()["id"]
    await wait_turn(http, tid, lambda t: t["legs"]["long_in"]["status"] == "chasing")
    exl.hit_resting(qty=2)                                   # partial fill before the crash
    await asyncio.sleep(0.4)
    old_order = [o for o in exl.orders.values() if o["status"] == "PARTIALLY_FILLED"][0]["orderId"]
    await turn_agents.restart("A1")                          # agent process dies and comes back; the gateway re-sends the intent
    t = await wait_turn(http, tid, lambda t: t["legs"]["long_in"]["status"] == "chasing" and t["legs"]["long_in"].get("order") not in (None, old_order))
    resting = [o for o in exl.orders.values() if o["status"] in ("NEW", "PARTIALLY_FILLED")]
    assert exl.orders[old_order]["status"] == "CANCELED" and len(resting) == 1 and resting[0]["origQty"] == 3   # orphan gone, remainder re-quoted
    exl.hit_resting()
    t = await wait_turn(http, tid, lambda t: t["legs"]["mt5_short"]["status"] == "open")
    assert t["legs"]["long_in"]["filled"] == 5 and exl.pos["XAUUSDT"]["amt"] == 5
