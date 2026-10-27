"""In-memory Binance Futures stand-in with the same surface as exchange.Binance (one-way mode, USDT)."""
import asyncio, time
from collections import Counter
from .exchange import BinanceError


def _ms():
    return int(time.time() * 1000)


class Fake:
    testnet = True
    key = "fakekey0000"

    def __init__(self):
        self.prices = {"BTCUSDT": (60000.0, 60001.0), "ETHUSDT": (3000.0, 3000.5)}  # bid, ask
        self.wallet = 10000.0
        self.pos: dict[str, dict] = {}   # symbol -> {"amt", "entry"}
        self.orders: dict[int, dict] = {}      # orderId -> binance-like order
        self.events: asyncio.Queue = asyncio.Queue()
        self.calls = Counter()
        self.fail_next = None   # (code, msg) raised by the next place/cancel
        self.delay = 0.0
        self._oid, self._tid = 1000, 5000

    # ---- helpers
    def _mark(self, s):
        b, a = self.prices[s]
        return (a + b) / 2

    def _upnl(self, s, p):
        return (self._mark(s) - p["entry"]) * p["amt"]

    def _fail(self):
        if self.fail_next:
            code, msg = self.fail_next
            self.fail_next = None
            raise BinanceError(code, msg)

    async def _emit_order(self, o, x, extra=None):
        self.events.put_nowait({"e": "ORDER_TRADE_UPDATE", "E": _ms(), "T": _ms(), "o": {
            "s": o["symbol"], "c": o["clientOrderId"], "S": o["side"], "o": o["type"], "q": str(o["origQty"]),
            "p": str(o["price"]), "ap": str(o["avgPrice"]), "x": x, "X": o["status"], "i": o["orderId"],
            "z": str(o["executedQty"]), "R": o["reduceOnly"], "cp": o["closePosition"], "sp": str(o["stopPrice"]),
            "ps": "BOTH", "T": _ms(), **(extra or {})}})

    def _emit_account(self):
        self.events.put_nowait({"e": "ACCOUNT_UPDATE", "E": _ms(), "a": {
            "B": [{"a": "USDT", "wb": str(self.wallet), "cw": str(self.wallet)}],
            "P": [{"s": s, "pa": str(p["amt"]), "ep": str(p["entry"]), "up": str(self._upnl(s, p)), "ps": "BOTH"}
                  for s, p in self.pos.items()]}})

    # ---- REST surface
    async def exchange_info(self):
        self.calls["exchange_info"] += 1
        return {"symbols": [{"symbol": s, "status": "TRADING"} for s in self.prices]}

    async def balance(self):
        upnl = sum(self._upnl(s, p) for s, p in self.pos.items())
        margin = sum(abs(p["amt"]) * p["entry"] / 20 for p in self.pos.values())
        return [{"asset": "USDT", "balance": str(self.wallet), "crossWalletBalance": str(self.wallet),
                 "crossUnPnl": str(upnl), "availableBalance": str(self.wallet + upnl - margin)}]

    async def positions(self):
        return [{"symbol": s, "positionSide": "BOTH", "positionAmt": str(p["amt"]), "entryPrice": str(p["entry"]),
                 "markPrice": str(self._mark(s)), "unRealizedProfit": str(self._upnl(s, p)),
                 "initialMargin": str(abs(p["amt"]) * p["entry"] / 20), "leverage": "20", "updateTime": _ms()}
                for s, p in self.pos.items() if p["amt"]]

    async def open_orders(self, symbol=None):
        return [o for o in self.orders.values() if o["status"] == "NEW" and (symbol is None or o["symbol"] == symbol)]

    async def place_order(self, **p):
        self.calls["place_order"] += 1
        await asyncio.sleep(self.delay)
        self._fail()
        self._oid += 1
        s, side, type_ = p["symbol"], p["side"], p["type"]
        qty = float(p.get("quantity") or 0)
        o = {"orderId": self._oid, "symbol": s, "status": "NEW", "clientOrderId": p.get("newClientOrderId", f"auto{self._oid}"),
             "price": float(p.get("price") or 0), "avgPrice": 0.0, "origQty": qty, "executedQty": 0.0, "type": type_,
             "side": side, "reduceOnly": bool(p.get("reduceOnly", False)), "closePosition": bool(p.get("closePosition", False)),
             "stopPrice": float(p.get("stopPrice") or 0), "updateTime": _ms()}
        self.orders[o["orderId"]] = o
        if type_ != "MARKET":
            await self._emit_order(o, "NEW")
            return dict(o)
        bid, ask = self.prices[s]
        px = ask if side == "BUY" else bid
        pos = self.pos.setdefault(s, {"amt": 0.0, "entry": 0.0})
        delta = qty if side == "BUY" else -qty
        if o["reduceOnly"]:
            delta = max(-abs(pos["amt"]), min(abs(pos["amt"]), delta)) if pos["amt"] * delta < 0 else 0.0
        rp = 0.0
        if pos["amt"] * delta < 0:  # reducing
            closed = min(abs(delta), abs(pos["amt"]))
            rp = (px - pos["entry"]) * closed * (1 if pos["amt"] > 0 else -1)
            self.wallet += rp
        new_amt = round(pos["amt"] + delta, 8)
        if pos["amt"] * delta >= 0 and delta:  # adding
            pos["entry"] = (pos["entry"] * abs(pos["amt"]) + px * abs(delta)) / (abs(pos["amt"]) + abs(delta))
        elif new_amt and pos["amt"] * new_amt < 0:
            pos["entry"] = px
        pos["amt"] = new_amt
        if not new_amt:
            self.pos.pop(s, None)
        fee = abs(delta) * px * 0.0004
        self.wallet -= fee
        self._tid += 1
        o.update(status="FILLED", avgPrice=px, executedQty=abs(delta))
        await self._emit_order(o, "TRADE", {"l": str(abs(delta)), "L": str(px), "n": str(fee), "N": "USDT", "t": self._tid, "rp": str(rp)})
        self._emit_account()
        return dict(o)

    async def cancel_order(self, symbol, order_id):
        self.calls["cancel_order"] += 1
        self._fail()
        o = self.orders.get(int(order_id))
        if not o or o["status"] != "NEW":
            raise BinanceError(-2011, "Unknown order sent.")
        o["status"] = "CANCELED"
        await self._emit_order(o, "CANCELED")
        return dict(o)

    async def listen_key(self):
        return "fake-listen-key"

    async def keepalive(self, key):
        self.calls["keepalive"] += 1

    async def user_stream(self, key):
        while True:
            yield await self.events.get()

    async def book_ticker(self, symbols):
        self.calls["book_ticker"] += 1
        while True:
            for s in symbols:
                b, a = self.prices[s]
                yield {"s": s, "b": str(b), "a": str(a), "E": _ms()}
            await asyncio.sleep(0.05)

    async def aclose(self):
        pass
