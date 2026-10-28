"""End-to-end: real gateway + Binance trader/streamer on the in-memory fake exchange."""
import asyncio, time
import pytest

from conftest import API  # noqa: E402

ORDER = {"symbol": "BTCUSDT", "side": "buy", "volume": 0.01, "magic": 7}


@pytest.fixture
def ex():
    from binance.fake import Fake
    return Fake()


@pytest.fixture
async def agents(server, ex):
    from binance import streamer, trader
    base = server.replace("http", "ws")
    t = asyncio.create_task(trader.run(base + "/agents/A1/trader", "tokA1", ex, order_timeout=1.0, ttl=5.0))
    s = asyncio.create_task(streamer.run(base + "/agents/A1/streamer", "tokA1", ex, state_period=0.1, reconcile_period=0.5))
    await asyncio.sleep(0.4)
    yield
    t.cancel(); s.cancel()


async def collect(channels, want, timeout=3.0):
    from gateway import bus
    got = []
    async with asyncio.timeout(timeout):
        async for ev, data in bus.subscribe(channels):
            if ev is None:
                continue
            got.append((ev, data))
            if want(got):
                return got


async def test_market_open_returns_done_with_fill(server, http, ex, agents):
    r = (await http.post("/accounts/A1/orders", json=ORDER)).json()
    assert r["status"] == "done" and r["retcode"] == 0 and r["order"] > 0
    assert r["price"] == ex.prices["BTCUSDT"][1] and r["volume"] == 0.01  # bought at ask
    assert r["timing"]["mt5"] >= 0 and r["t_recv"] > 0


async def test_state_shows_position_and_deal_is_published(server, http, ex, agents):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "deal" for e, _ in g)))
    await asyncio.sleep(0.1)
    await http.post("/accounts/A1/orders", json=ORDER)
    deal = dict(await task)["deal"]
    assert deal["symbol"] == "BTCUSDT" and deal["volume"] == 0.01 and deal["magic"] == 7 and deal["entry"] == 0
    await asyncio.sleep(0.3)
    s = (await http.get("/accounts/A1/state")).json()
    p = s["positions"][0]
    assert p["ticket"] == "BTCUSDT" and p["type"] == 0 and p["volume"] == 0.01 and s["account"]["currency"] == "USDT"


async def test_close_position_by_symbol(server, http, ex, agents):
    await http.post("/accounts/A1/orders", json=ORDER)
    r = (await http.post("/accounts/A1/positions/BTCUSDT/close", json={})).json()
    assert r["status"] == "done" and r["volume"] == 0.01
    await asyncio.sleep(0.3)
    assert (await http.get("/accounts/A1/state")).json()["positions"] == []


async def test_open_with_sl_tp_places_close_position_algos_and_modify_replaces_them(server, http, ex, agents):
    r = (await http.post("/accounts/A1/orders", json={**ORDER, "sl": 50000, "tp": 70000})).json()
    assert r["status"] == "done"
    algos = [o for o in ex.orders.values() if o["closePosition"]]
    assert {o["type"] for o in algos} == {"STOP_MARKET", "TAKE_PROFIT_MARKET"}
    r = (await http.patch("/accounts/A1/positions/BTCUSDT", json={"sl": 55000})).json()
    assert r["status"] == "done"
    sl = [o for o in ex.orders.values() if o["closePosition"] and o["type"] == "STOP_MARKET" and o["status"] == "NEW"]
    assert len(sl) == 1 and sl[0]["stopPrice"] == 55000
    await asyncio.sleep(0.3)
    p = (await http.get("/accounts/A1/state")).json()["positions"][0]
    assert p["sl"] == 55000 and p["tp"] == 70000


async def test_limit_order_then_cancel(server, http, ex, agents):
    r = (await http.post("/accounts/A1/orders", json={**ORDER, "type": "limit", "price": 50000})).json()
    assert r["status"] == "done" and r["volume"] == 0  # resting, nothing filled
    await asyncio.sleep(0.3)
    s = (await http.get("/accounts/A1/state")).json()
    assert s["orders"][0]["order"] == r["order"] and s["positions"] == []
    r2 = (await http.post("/accounts/A1/orders/cancel", json={"symbol": "BTCUSDT", "order": r["order"]})).json()
    assert r2["status"] == "done" and ex.orders[r["order"]]["status"] == "CANCELED"


async def test_exchange_error_maps_to_retcode(server, http, ex, agents):
    ex.fail_next = (-2019, "Margin is insufficient.")
    r = (await http.post("/accounts/A1/orders", json=ORDER)).json()
    assert r["status"] == "error" and r["retcode"] == -2019 and "insufficient" in r["comment"]


async def test_ticks_stream_on_subscribe(server, http, ex, agents):
    async with http.stream("GET", "/accounts/A1/stream?symbols=BTCUSDT") as resp:
        async with asyncio.timeout(3):
            async for line in resp.aiter_lines():
                if line.startswith("data:") and '"bid"' in line:
                    break
    assert ex.calls["book_ticker"] >= 1


async def test_symbols_list_from_exchange_info(server, http, ex, agents):
    r = await http.get("/accounts/A1/symbols")
    assert r.status_code == 200 and "BTCUSDT" in r.json()["symbols"]


async def test_state_follows_the_funded_margin_asset(server, http, ex, agents):
    ex.asset = "USDC"   # account funded in USDC only: state must report USDC, not a zero USDT line
    await asyncio.sleep(0.7)  # next reconcile
    s = (await http.get("/accounts/A1/state")).json()
    assert s["account"]["currency"] == "USDC" and s["account"]["balance"] == ex.wallet


# ---- stage 1: leverage, spec, chase ----

async def test_leverage_route_sets_symbol_leverage(server, http, ex, agents):
    r = (await http.post("/accounts/A1/leverage", json={"symbol": "BTCUSDT", "leverage": 7})).json()
    assert r["status"] == "done" and ex.leverage["BTCUSDT"] == 7


async def test_symbol_spec_is_normalised(server, http, ex, agents):
    r = (await http.get("/accounts/A1/symbols/BTCUSDT")).json()
    assert r == {"symbol": "BTCUSDT", "base": "BTC", "quote": "USDT", "contract_size": 1, "volume_min": 0.001,
                 "volume_step": 0.001, "volume_max": 1000, "price_step": 0.1, "min_notional": 5}


async def start_chase(http, side="buy", volume=0.01, interval_ms=100, **extra):
    r = await http.post("/accounts/A1/orders/chase", json={"symbol": "BTCUSDT", "side": side, "volume": volume,
                                                            "interval_ms": interval_ms, "magic": 7, **extra})
    assert r.status_code == 202, r.text
    return r.json()["id"]


async def test_chase_places_post_only_at_queue_and_fills_when_hit(server, http, ex, agents):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "result" and d.get("filled") for e, d in g)))
    await asyncio.sleep(0.1)
    cid = await start_chase(http)
    await asyncio.sleep(0.15)
    resting = [o for o in ex.orders.values() if o["status"] == "NEW"]
    assert len(resting) == 1 and resting[0]["timeInForce"] == "GTX" and resting[0]["price"] == ex.prices["BTCUSDT"][0]  # at bid
    ex.hit_resting()  # a seller takes our bid
    res = [d for e, d in await task if e == "result" and d["id"] == cid][0]
    assert res["status"] == "done" and res["filled"] == 0.01 and res["avg_price"] == ex.prices["BTCUSDT"][0]
    assert res["moves"] >= 0 and res["timing"]["total"] > 0


async def test_chase_moves_with_best_bid_and_can_be_canceled(server, http, ex, agents):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "result" and "filled" in d for e, d in g)))
    await asyncio.sleep(0.1)
    cid = await start_chase(http, interval_ms=5000)   # long interval: only a best-price change may move it
    await asyncio.sleep(0.15)
    ex.prices["BTCUSDT"] = (60010.0, 60011.0)
    await asyncio.sleep(0.3)
    resting = [o for o in ex.orders.values() if o["status"] == "NEW"]
    assert ex.calls["cancel_order"] >= 1 and len(resting) == 1 and resting[0]["price"] == 60010.0
    r = await http.post(f"/accounts/A1/commands/{cid}/cancel")
    assert r.status_code == 200
    res = [d for e, d in await task if e == "result" and d["id"] == cid][0]
    assert res["status"] == "canceled" and res["filled"] == 0 and res["moves"] >= 1
    assert not [o for o in ex.orders.values() if o["status"] == "NEW"]


async def test_chase_retries_when_post_only_would_cross(server, http, ex, agents):
    ex.reject_post_only_once = True
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "result" and "filled" in d for e, d in g)))
    await asyncio.sleep(0.1)
    cid = await start_chase(http)
    await asyncio.sleep(0.2)
    ex.hit_resting()
    res = [d for e, d in await task if e == "result" and d["id"] == cid][0]
    assert res["status"] == "done" and res["filled"] == 0.01 and ex.calls["place_order"] >= 2


async def test_chase_keeps_partial_fill_on_cancel_and_emits_updates(server, http, ex, agents):
    task = asyncio.create_task(collect(["acct:A1"], lambda g: any(e == "result" and "filled" in d for e, d in g)))
    await asyncio.sleep(0.1)
    cid = await start_chase(http, volume=0.04, interval_ms=5000)
    await asyncio.sleep(0.15)
    ex.hit_resting(qty=0.01)   # partial
    await asyncio.sleep(0.1)
    await http.post(f"/accounts/A1/commands/{cid}/cancel")
    got = await task
    res = [d for e, d in got if e == "result" and d["id"] == cid][0]
    assert res["status"] == "canceled" and res["filled"] == 0.01 and res["remaining"] == 0.03
    assert any(e == "update" and d["id"] == cid and d["price"] for e, d in got)
