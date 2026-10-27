"""Binance streamer: account state (REST + user stream), deals, on-demand bookTicker ticks. Never trades."""
import asyncio, json, logging, os, time
from agent.link import Link, heartbeat, log

KEEPALIVE = 30 * 60


def _f(x):
    return float(x or 0)


class State:
    """Mutable mirror of the account; `snapshot()` renders the gateway `state` payload."""

    def __init__(self, ex):
        self.ex, self.wallet, self.available, self.margin = ex, 0.0, 0.0, 0.0
        self.asset = os.environ.get("BINANCE_ASSET") or None   # margin asset to report; default: the one with money in it
        self.positions: dict[str, dict] = {}   # symbol -> {amt, entry, mark, upnl, leverage}
        self.orders: dict[int, dict] = {}      # NEW orders
        self.connected = False

    async def reconcile(self):
        balances = await self.ex.balance()
        if self.asset is None and balances:
            self.asset = max(balances, key=lambda b: _f(b["balance"]))["asset"]
        bal = next((b for b in balances if b["asset"] == self.asset), None)
        if bal:
            self.wallet, self.available = _f(bal["balance"]), _f(bal["availableBalance"])
        self.positions = {p["symbol"]: {"amt": _f(p["positionAmt"]), "entry": _f(p["entryPrice"]), "mark": _f(p.get("markPrice")),
                                        "upnl": _f(p["unRealizedProfit"]), "leverage": int(_f(p.get("leverage")) or 0)}
                          for p in await self.ex.positions() if _f(p["positionAmt"])}
        self.margin = sum(_f(p.get("initialMargin")) for p in await self.ex.positions())
        self.orders = {o["orderId"]: o for o in await self.ex.open_orders()}
        self.connected = True

    def apply(self, ev):
        """Returns a deal dict when the event is a fill, else None."""
        if ev.get("e") == "ACCOUNT_UPDATE":
            for b in ev["a"].get("B", []):
                if b["a"] == self.asset:
                    self.wallet = _f(b["wb"])
            for p in ev["a"].get("P", []):
                amt = _f(p["pa"])
                if amt:
                    cur = self.positions.setdefault(p["s"], {"amt": 0, "entry": 0, "mark": 0, "upnl": 0, "leverage": 0})
                    cur.update(amt=amt, entry=_f(p["ep"]), upnl=_f(p.get("up")))
                else:
                    self.positions.pop(p["s"], None)
        elif ev.get("e") == "ORDER_TRADE_UPDATE":
            o = ev["o"]
            if o["X"] == "NEW":
                self.orders[o["i"]] = {"orderId": o["i"], "symbol": o["s"], "side": o["S"], "type": o["o"], "price": o["p"],
                                       "stopPrice": o.get("sp", "0"), "origQty": o["q"], "executedQty": o["z"], "status": "NEW",
                                       "clientOrderId": o["c"], "closePosition": o.get("cp", False)}
            else:
                self.orders.pop(o["i"], None)
            if o["x"] == "TRADE":
                magic = int(o["c"][1:].split("-")[0]) if o["c"].startswith("m") and o["c"][1:].split("-")[0].isdigit() else 0
                return {"ticket": o.get("t"), "order": o["i"], "symbol": o["s"], "side": o["S"].lower(),
                        "entry": 1 if (o.get("R") or o.get("cp")) else 0, "volume": _f(o.get("l")), "price": _f(o.get("L")),
                        "profit": _f(o.get("rp")), "commission": _f(o.get("n")), "time_msc": o.get("T"), "magic": magic,
                        "client": o["c"]}
        return None

    def snapshot(self):
        algos = {}
        for o in self.orders.values():
            if o.get("closePosition"):
                algos.setdefault(o["symbol"], {})["sl" if o["type"] == "STOP_MARKET" else "tp"] = _f(o.get("stopPrice"))
        upnl = sum(p["upnl"] for p in self.positions.values())
        return {
            "account": {"login": self.ex.key[:6], "server": "binance-futures" + ("-testnet" if getattr(self.ex, "testnet", False) else ""),
                        "currency": self.asset or "USDT", "balance": self.wallet, "equity": self.wallet + upnl, "margin": self.margin,
                        "margin_free": self.available, "profit": upnl},
            "positions": [{"ticket": s, "symbol": s, "type": 0 if p["amt"] > 0 else 1, "volume": abs(p["amt"]), "price_open": p["entry"],
                           "price_current": p["mark"], "profit": p["upnl"], "sl": algos.get(s, {}).get("sl"), "tp": algos.get(s, {}).get("tp"),
                           "magic": 0, "leverage": p["leverage"]} for s, p in sorted(self.positions.items())],
            "orders": [{"order": o["orderId"], "symbol": o["symbol"], "side": o["side"].lower(), "type": o["type"], "price": _f(o["price"]),
                        "stop": _f(o.get("stopPrice")), "qty": _f(o["origQty"]), "filled": _f(o["executedQty"]), "status": o["status"],
                        "client": o.get("clientOrderId")} for o in sorted(self.orders.values(), key=lambda o: o["orderId"])],
            "connected": self.connected,
        }


async def run(url, token, ex, state_period=0.25, reconcile_period=30.0):
    link, st, subscribed = Link(url, token), State(ex), set()
    last = {"json": None, "at": 0.0}
    ticks = {"task": None}

    def publish_state(force=False):
        snap = st.snapshot()
        js = json.dumps(snap, sort_keys=True)
        if force or js != last["json"] or time.time() - last["at"] > reconcile_period:
            last.update(json=js, at=time.time())
            link.emit("state", snap)

    async def reconcile_loop():
        while True:
            try:
                await st.reconcile()
            except Exception as e:
                st.connected = False
                log.warning("reconcile failed: %r", e)
            publish_state(force=True)
            await asyncio.sleep(reconcile_period)

    async def user_stream_loop():
        while True:
            try:
                key = await ex.listen_key()
                ka = asyncio.create_task(_keepalive(ex, key))
                try:
                    async for ev in ex.user_stream(key):
                        deal = st.apply(ev)
                        if deal:
                            link.emit("deal", deal)
                        publish_state()
                finally:
                    ka.cancel()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("user stream error: %r", e)
            await asyncio.sleep(2)

    async def tick_loop(symbols):
        try:
            async for t in ex.book_ticker(sorted(symbols)):
                link.emit("tick", {"symbol": t["s"], "bid": _f(t["b"]), "ask": _f(t["a"]), "time_msc": t.get("E")})
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.warning("bookTicker error: %r", e)
            await asyncio.sleep(2)
            if subscribed:
                ticks["task"] = asyncio.create_task(tick_loop(set(subscribed)))

    def restart_ticks():
        if ticks["task"]:
            ticks["task"].cancel()
        ticks["task"] = asyncio.create_task(tick_loop(set(subscribed))) if subscribed else None

    async def on_message(msg):
        syms = msg.get("payload", {}).get("symbols", [])
        if msg["type"] == "subscribe":
            subscribed.update(syms); restart_ticks()
        elif msg["type"] == "unsubscribe":
            subscribed.difference_update(syms); restart_ticks()

    async def state_loop():
        while True:
            publish_state()
            await asyncio.sleep(state_period)

    await asyncio.gather(link.run(on_message), reconcile_loop(), user_stream_loop(), state_loop(),
                         heartbeat(link.emit, lambda: {"connected": st.connected, "subscribed": sorted(subscribed)}))


async def _keepalive(ex, key):
    while True:
        await asyncio.sleep(KEEPALIVE)
        await ex.keepalive(key)
