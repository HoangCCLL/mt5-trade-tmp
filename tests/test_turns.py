"""Trade turn orchestration: two Binance accounts (fake exchanges) + one MT5 account (fake terminal), real gateway."""
import asyncio, os
import pytest

os.environ["MT5_FAKE"] = "1"


@pytest.fixture
async def turn_agents(server):
    from binance.fake import Fake
    from binance import trader as btrader
    from agent import trader as mtrader
    from agent.mt5_api import mt5
    mt5.reset()
    exl, exs = Fake(), Fake()
    for ex in (exl, exs):
        ex.prices["XAUUSDT"] = (4000.0, 4000.5)
    base = server.replace("http", "ws")
    tasks = [asyncio.create_task(btrader.run(base + "/agents/A1/trader", "tokA1", exl, order_timeout=1.0, ttl=5.0)),
             asyncio.create_task(btrader.run(base + "/agents/A2/trader", "tokA2", exs, order_timeout=1.0, ttl=5.0)),
             asyncio.create_task(mtrader.run(base + "/agents/A3/trader", "tokA3", order_timeout=1.0, ttl=5.0))]
    await asyncio.sleep(0.4)
    yield exl, exs, mt5
    for t in tasks:
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
    r = await http.post("/turns", json={"units": 5, "leverage": 10, "interval_ms": 5000})
    assert r.status_code == 201, r.text
    t = r.json(); tid = t["id"]
    assert t["volume"] == {"binance": 5, "mt5": 0.05} and t["status"] == "opening"
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
    tid = (await http.post("/turns", json={"units": 5, "leverage": 10, "interval_ms": 5000})).json()["id"]
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
    tid = (await http.post("/turns", json={"units": 5, "leverage": 10, "interval_ms": 5000})).json()["id"]
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
