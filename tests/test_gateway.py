import asyncio, json
import httpx, pytest
from conftest import FakeAgent

ORDER = {"symbol": "EURUSD", "side": "buy", "volume": 0.1, "magic": 7}


async def test_order_returns_result_from_trader(server, http):
    async with FakeAgent(server, "A1", "trader", result_delay=0.2) as t:
        r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "done"
    cmd = t.received[0]
    assert cmd["type"] == "order.open" and cmd["payload"]["side"] == "buy"


async def test_order_without_trader_is_503(server, http):
    r = await http.post("/accounts/A1/orders", json=ORDER)
    assert r.status_code == 503


async def test_idempotency_key_becomes_command_id(server, http):
    async with FakeAgent(server, "A1", "trader") as t:
        await http.post("/accounts/A1/orders", json=ORDER, headers={"Idempotency-Key": "my-id"})
    assert t.received[0]["id"] == "my-id"


async def test_late_result_gives_504_then_is_published(server, http):
    from gateway import bus
    async with FakeAgent(server, "A1", "trader", result_delay=2.0):
        got = []

        async def listen():
            async for ev, data in bus.subscribe(["acct:A1"], idle=0.5):
                if ev == "result":
                    got.append(data); return
        lt = asyncio.create_task(listen())
        r = await http.post("/accounts/A1/orders", json=ORDER)
        assert r.status_code == 504
        cid = r.json()["detail"]["id"]
        await asyncio.wait_for(lt, 4)
    assert got[0]["id"] == cid


async def test_streamer_state_is_saved_and_readable(server, http):
    state = {"account": {"balance": 100.0}, "positions": [], "connected": True}
    async with FakeAgent(server, "A1", "streamer") as s:
        await s.send("state", state)
        await asyncio.sleep(0.1)
        r = await http.get("/accounts/A1/state")
    assert r.status_code == 200 and r.json() == state


async def test_sse_sends_snapshot_then_live_events(server, http):
    async with FakeAgent(server, "A1", "streamer") as s:
        await s.send("state", {"account": {}, "positions": [], "connected": True})
        await asyncio.sleep(0.1)
        events = []
        async with http.stream("GET", "/accounts/A1/stream?symbols=EURUSD") as resp:
            lines = resp.aiter_lines()
            async def next_event():
                ev = {}
                async for line in lines:
                    if line.startswith("event:"): ev["event"] = line[6:].strip()
                    elif line.startswith("data:"): ev["data"] = json.loads(line[5:])
                    elif line == "" and ev: return ev
            events.append(await next_event())
            await s.wait_for("subscribe")
            await s.send("tick", {"symbol": "EURUSD", "bid": 1.1, "ask": 1.2, "time_msc": 1})
            events.append(await next_event())
    assert events[0]["event"] == "state"
    assert events[1]["event"] == "tick" and events[1]["data"]["bid"] == 1.1


async def test_tick_refcount_subscribes_once_and_unsubscribes_after_grace(server, http):
    async with FakeAgent(server, "A1", "streamer") as s:
        async def viewer():
            async with http.stream("GET", "/accounts/A1/stream?symbols=XAUUSD") as resp:
                await resp.aiter_lines().__anext__()
                await asyncio.sleep(0.3)
        await asyncio.gather(viewer(), viewer())
        subs = [m for m in s.received if m["type"] == "subscribe"]
        assert len(subs) == 1 and subs[0]["payload"]["symbols"] == ["XAUUSD"]
        await asyncio.sleep(0.2)
        assert not [m for m in s.received if m["type"] == "unsubscribe"], "unsubscribed before grace"
        await s.wait_for("unsubscribe", timeout=2)


async def test_streamer_reconnect_gets_resubscribe(server, http):
    async with FakeAgent(server, "A1", "streamer") as s:
        async with http.stream("GET", "/accounts/A1/stream?symbols=EURUSD,GBPUSD") as resp:
            await resp.aiter_lines().__anext__()
            await s.wait_for("subscribe")
            async with FakeAgent(server, "A1", "streamer") as s2:
                m = await s2.wait_for("subscribe")
                assert sorted(m["payload"]["symbols"]) == ["EURUSD", "GBPUSD"]


async def test_second_trader_replaces_first(server, http):
    async with FakeAgent(server, "A1", "trader") as t1:
        async with FakeAgent(server, "A1", "trader") as t2:
            await asyncio.sleep(0.1)
            assert t1.closed
            await http.post("/accounts/A1/orders", json=ORDER)
            assert t2.received and not t1.received


async def test_bad_token_rejected(server):
    with pytest.raises(Exception):
        async with FakeAgent(server, "A1", "trader", token="wrong") as t:
            await t.ws.recv()


async def test_accounts_list_shows_connection_and_alive(server, http):
    async with FakeAgent(server, "A1", "streamer") as s:
        await s.send("heartbeat", {"connected": True})
        await asyncio.sleep(0.1)
        r = await http.get("/accounts")
    row = next(x for x in r.json() if x["id"] == "A1")
    assert row["streamer"] is True and row["alive"] is True


async def test_healthz_needs_no_api_key(server):
    async with httpx.AsyncClient(base_url=server) as c:
        r = await c.get("/healthz")
    assert r.status_code == 200 and r.json() == {"ok": True}


async def test_trader_heartbeat_is_answered_with_hello(server):
    async with FakeAgent(server, "A1", "trader") as t:
        await t.send("heartbeat", {"connected": True})
        async with asyncio.timeout(2):
            while t.hellos < 2:  # one on connect, one per heartbeat
                await asyncio.sleep(0.02)
