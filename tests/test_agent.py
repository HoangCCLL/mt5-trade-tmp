"""End-to-end: real gateway + real trader/streamer running on the fake MT5 backend."""
import asyncio, os, time
import pytest

os.environ["MT5_FAKE"] = "1"

from conftest import API  # noqa: E402

ORDER = {"symbol": "EURUSD", "side": "buy", "volume": 0.1, "magic": 7}


@pytest.fixture
def mt5():
    from agent.mt5_api import mt5
    mt5.reset()
    return mt5


@pytest.fixture
async def trader(server, mt5):
    from agent import trader
    t = asyncio.create_task(trader.run(server.replace("http", "ws") + "/agents/A1/trader", "tokA1",
                                       order_timeout=1.0, ttl=5.0))
    await asyncio.sleep(0.2)
    yield t
    t.cancel()


@pytest.fixture
async def streamer(server, mt5):
    from agent import streamer
    s = asyncio.create_task(streamer.run(server.replace("http", "ws") + "/agents/A1/streamer", "tokA1",
                                         state_period=0.05, tick_period=0.05, deal_period=0.1))
    await asyncio.sleep(0.3)
    yield s
    s.cancel()


async def collect(channels, want, timeout=3.0):
    from gateway import bus
    got = []
    async with asyncio.timeout(timeout):
        async for ev, data in bus.subscribe(channels, idle=0.2):
            if ev is not None:
                got.append((ev, data))
                if want(got):
                    return got


# ---------------- trader ----------------

async def test_trader_opens_position_and_returns_done(server, http, mt5, trader):
    r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["status"] == "done" and body["retcode"] == mt5.TRADE_RETCODE_DONE and body["order"] > 0
    pos = mt5.positions_get()
    assert len(pos) == 1 and pos[0].magic == 7 and pos[0].volume == 0.1


async def test_trader_sends_ack_before_result(server, http, mt5, trader):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "result" for e, _ in g)))
    await asyncio.sleep(0.1)
    await http.post("/accounts/A1/orders", json=ORDER)
    events = [e for e, _ in await task]
    assert events.index("ack") < events.index("result")


async def test_trader_executes_duplicate_id_once(server, http, mt5, trader):
    h = {"Idempotency-Key": "dup-1"}
    r1 = await http.post("/accounts/A1/orders", json=ORDER, headers=h)
    r2 = await http.post("/accounts/A1/orders", json=ORDER, headers=h)
    assert r1.status_code == r2.status_code == 200
    assert r1.json()["order"] == r2.json()["order"]
    assert mt5.calls["order_send"] == 1


async def test_trader_rejects_expired_command(mt5):
    from agent import trader
    out = []
    await trader.handle_command({"id": "old", "type": "order.open", "ts": time.time() - 10, "payload": ORDER},
                                emit=lambda t, p: out.append((t, p)), ttl=5.0, order_timeout=1.0)
    assert out == [("result", {"id": "old", "status": "rejected", "comment": "ttl_expired"})]
    assert mt5.calls["order_send"] == 0


async def test_trader_reports_unknown_when_broker_is_slow(server, http, mt5, trader):
    mt5.order_send_delay = 2.0
    r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.status_code == 504
    cid = r.json()["detail"]["id"]
    got = await collect(["acct:A1"], lambda g: len([1 for e, d in g if e == "result" and d["id"] == cid]) == 2, timeout=5)
    statuses = [d["status"] for e, d in got if e == "result"]
    assert statuses == ["unknown", "done"]


async def test_trader_closes_and_modifies_position(server, http, mt5, trader):
    ticket = (await http.post("/accounts/A1/orders", json=ORDER)).json()["order"]
    r = await http.patch(f"/accounts/A1/positions/{ticket}", json={"sl": 1.0, "tp": 1.5})
    assert r.json()["status"] == "done" and mt5.positions_get()[0].sl == 1.0
    r = await http.post(f"/accounts/A1/positions/{ticket}/close")
    assert r.json()["status"] == "done" and mt5.positions_get() == ()


async def test_trader_market_closed_is_error(server, http, mt5, trader):
    mt5.market_closed = True
    r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.json()["status"] == "error" and r.json()["retcode"] == mt5.TRADE_RETCODE_MARKET_CLOSED


async def test_history_fill_streams_batches_via_trader(server, http, mt5, trader):
    for _ in range(3):
        await http.post("/accounts/A1/orders", json=ORDER)
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "history" for e, _ in g)))
    await asyncio.sleep(0.1)
    assert (await http.post("/accounts/A1/history/fill")).status_code == 202
    hist = [d for e, d in await task if e == "history"][0]
    assert hist["total"] == 3 and len(hist["deals"]) == 3


# ---------------- streamer ----------------

async def test_streamer_publishes_initial_state(server, http, mt5, streamer):
    r = await http.get("/accounts/A1/state")
    assert r.status_code == 200
    assert r.json()["connected"] is True and r.json()["account"]["balance"] == 10000.0


async def test_streamer_is_quiet_when_nothing_changes(server, http, mt5, streamer):
    with pytest.raises(TimeoutError):
        await collect(["acct:A1"], lambda g: any(e == "state" for e, _ in g), timeout=0.5)


async def test_streamer_emits_state_and_deal_after_fill(server, http, mt5, streamer):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: {"state", "deal"} <= {e for e, _ in g}))
    await asyncio.sleep(0.1)
    mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": "EURUSD", "volume": 0.2,
                    "type": mt5.ORDER_TYPE_BUY, "price": 1.1, "magic": 9})
    got = dict(await task)
    assert got["state"]["positions"][0]["magic"] == 9
    assert got["deal"]["magic"] == 9 and got["deal"]["volume"] == 0.2


async def test_streamer_ticks_on_subscribe_and_dedupes(server, http, mt5, streamer):
    async with http.stream("GET", "/accounts/A1/stream?symbols=EURUSD") as resp:
        ticks = []
        async with asyncio.timeout(3):
            async for line in resp.aiter_lines():
                if line.startswith("event: tick"):
                    ticks.append(line)
                    if len(ticks) == 3:
                        break
        mt5.freeze_ticks = True
        before = mt5.calls["symbol_info_tick"]
        await asyncio.sleep(0.4)
        assert mt5.calls["symbol_info_tick"] > before, "poller stopped"
    with pytest.raises(TimeoutError):
        await collect(["ticks:A1:EURUSD"], lambda g: True, timeout=0.4)


async def test_trader_reports_error_when_execute_raises(server, http, mt5, trader, monkeypatch):
    from agent import trader as tr
    tr._symbol_cache.clear()
    monkeypatch.setattr(mt5, "symbol_info", lambda s: (_ for _ in ()).throw(RuntimeError("boom")))
    t0 = time.time()
    r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.status_code == 200 and r.json()["status"] == "error" and "boom" in r.json()["comment"]
    assert time.time() - t0 < 0.9  # not the gateway RESULT_TIMEOUT


async def test_streamer_emits_deal_when_broker_clock_is_ahead(server, http, mt5, streamer):
    mt5.clock_offset = 3 * 3600  # broker server time (e.g. EET) vs VPS/UTC clock
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "deal" for e, _ in g)))
    await asyncio.sleep(0.1)
    mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": "EURUSD", "volume": 0.2,
                    "type": mt5.ORDER_TYPE_BUY, "price": 1.1, "magic": 9})
    assert dict(await task)["deal"]["magic"] == 9


async def test_streamer_backs_off_on_stale_ticks_despite_broker_clock_offset(server, http, mt5, streamer, monkeypatch):
    from agent import streamer as st
    monkeypatch.setattr(st, "STALE_AFTER", 0.2)
    monkeypatch.setattr(st, "STALE_PERIOD", 1.0)
    mt5.clock_offset = 3 * 3600  # broker stamps ticks 3h "ahead" of the VPS clock
    async with http.stream("GET", "/accounts/A1/stream?symbols=EURUSD") as resp:
        async with asyncio.timeout(3):
            async for line in resp.aiter_lines():
                if line.startswith("event: tick"):
                    break
        mt5.freeze_ticks = True
        await asyncio.sleep(0.3)
        before = mt5.calls["symbol_info_tick"]
        await asyncio.sleep(0.5)
        assert mt5.calls["symbol_info_tick"] - before <= 1, "poller did not back off on stale ticks"


async def test_trader_ttl_tolerates_skewed_vps_clock(server, http, mt5, monkeypatch):
    from agent import trader as tr
    monkeypatch.setattr(tr, "now", lambda: time.time() + 120)  # VPS clock two minutes fast
    t = asyncio.create_task(tr.run(server.replace("http", "ws") + "/agents/A1/trader", "tokA1", order_timeout=1.0, ttl=5.0))
    await asyncio.sleep(0.2)
    try:
        r = await http.post("/accounts/A1/orders", json=ORDER)
        assert r.json()["status"] == "done", r.json()
    finally:
        t.cancel()


async def test_history_fill_includes_deals_despite_broker_clock_offset(server, http, mt5, trader):
    mt5.clock_offset = 3 * 3600
    await http.post("/accounts/A1/orders", json=ORDER)
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "history" for e, _ in g)))
    await asyncio.sleep(0.1)
    await http.post("/accounts/A1/history/fill")
    hist = [d for e, d in await task if e == "history"][0]
    assert hist["total"] == 1


async def test_order_result_includes_timing_breakdown(server, http, mt5, trader):
    mt5.order_send_delay = 0.2
    r = (await http.post("/accounts/A1/orders", json=ORDER)).json()
    t = r["timing"]
    assert set(t) >= {"down", "pool", "mt5", "up", "total"}
    assert t["mt5"] >= 190 and t["total"] >= t["mt5"] and t["down"] >= 0 and t["up"] >= 0
