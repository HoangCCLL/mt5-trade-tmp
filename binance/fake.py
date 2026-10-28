"""In-memory Binance Futures stand-in with the same surface as exchange.Binance (one-way mode, USDT)."""
import asyncio, time
from collections import Counter
from .exchange import BinanceError


def _ms():
    return int(time.time() * 1000)


class Fake:
    testnet = True
    key = "fakekey0000"
    asset = "USDT"   # margin asset the fake account is funded in
    used_weight = 1  # what the real client reads from X-MBX-USED-WEIGHT-1M

    def __init__(self):
        self.prices = {"BTCUSDT": (60000.0, 60001.0), "ETHUSDT": (3000.0, 3000.5)}  # bid, ask
        self.wallet = 10000.0
        self.pos: dict[str, dict] = {}   # symbol -> {"amt", "entry"}
        self.orders: dict[int, dict] = {}      # orderId -> binance-like order
        self.events: asyncio.Queue = asyncio.Queue()
        self.calls = Counter()
        self.fail_next = None   # (code, msg) raised by the next place/cancel
        self.reject_post_only_once = False   # next GTX order is rejected as if it would cross
        self.delay = 0.0
        self.leverage: dict[str, int] = {}
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

    def _emit_order(self, o, x, extra=None):
        self.events.put_nowait({"e": "ORDER_TRADE_UPDATE", "E": _ms(), "T": _ms(), "o": {
            "s": o["symbol"], "c": o["clientOrderId"], "S": o["side"], "o": o["type"], "q": str(o["origQty"]),
            "p": str(o["price"]), "ap": str(o["avgPrice"]), "x": x, "X": o["status"], "i": o["orderId"],
            "z": str(o["executedQty"]), "R": o["reduceOnly"], "cp": o["closePosition"], "sp": str(o["stopPrice"]),
            "ps": "BOTH", "T": _ms(), **(extra or {})}})

    def _emit_account(self):
        self.events.put_nowait({"e": "ACCOUNT_UPDATE", "E": _ms(), "a": {
            "B": [{"a": self.asset, "wb": str(self.wallet), "cw": str(self.wallet)}],
            "P": [{"s": s, "pa": str(p["amt"]), "ep": str(p["entry"]), "up": str(self._upnl(s, p)), "ps": "BOTH"}
                  for s, p in self.pos.items()]}})

    # ---- REST surface
    async def exchange_info(self):
        self.calls["exchange_info"] += 1
        return {"symbols": [{"symbol": s, "status": "TRADING", "baseAsset": s[:-4], "quoteAsset": s[-4:], "filters": [
            {"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001", "maxQty": "1000"},
            {"filterType": "PRICE_FILTER", "tickSize": "0.10"}, {"filterType": "MIN_NOTIONAL", "notional": "5"}]} for s in self.prices]}

    async def balance(self):
        upnl = sum(self._upnl(s, p) for s, p in self.pos.items())
        margin = sum(abs(p["amt"]) * p["entry"] / 20 for p in self.pos.values())
        return [{"asset": self.asset, "balance": str(self.wallet), "crossWalletBalance": str(self.wallet),
                 "crossUnPnl": str(upnl), "availableBalance": str(self.wallet + upnl - margin)}]

    async def positions(self):
        return [{"symbol": s, "positionSide": "BOTH", "positionAmt": str(p["amt"]), "entryPrice": str(p["entry"]),
                 "markPrice": str(self._mark(s)), "unRealizedProfit": str(self._upnl(s, p)),
                 "initialMargin": str(abs(p["amt"]) * p["entry"] / 20), "leverage": "20", "updateTime": _ms(),
                 "liquidationPrice": str(p["entry"] * (0.95 if p["amt"] > 0 else 1.05))}
                for s, p in self.pos.items() if p["amt"]]

    async def open_orders(self, symbol=None):
        return [o for o in self.orders.values() if o["status"] in ("NEW", "PARTIALLY_FILLED") and (symbol is None or o["symbol"] == symbol)]

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
        if type_ == "LIMIT":
            bid, ask = self.prices[s]
            pm = p.get("priceMatch")
            if pm:
                o["price"] = (bid if side == "BUY" else ask) if pm.startswith("QUEUE") else (ask if side == "BUY" else bid)
            crosses = (side == "BUY" and o["price"] >= ask) or (side == "SELL" and o["price"] <= bid)
            if p.get("timeInForce") == "GTX" and (crosses or self.reject_post_only_once):
                self.reject_post_only_once = False
                self.orders.pop(o["orderId"])
                raise BinanceError(-5022, "Due to the order could not be executed as maker, the Post Only order will be rejected.")
        o["timeInForce"] = p.get("timeInForce", "GTC")
        if type_ != "MARKET":
            self._emit_order(o, "NEW")
            return dict(o)
        bid, ask = self.prices[s]
        qty_f, rp, fee = self._apply_fill(s, side, qty, ask if side == "BUY" else bid, o["reduceOnly"])
        self._tid += 1
        o.update(status="FILLED", avgPrice=ask if side == "BUY" else bid, executedQty=qty_f)
        self._emit_order(o, "TRADE", {"l": str(qty_f), "L": str(o["avgPrice"]), "n": str(fee), "N": "USDT", "t": self._tid, "rp": str(rp)})
        self._emit_account()
        return dict(o)

    def _apply_fill(self, s, side, qty, px, reduce_only):
        pos = self.pos.setdefault(s, {"amt": 0.0, "entry": 0.0})
        delta = qty if side == "BUY" else -qty
        if reduce_only:
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
        return abs(delta), rp, fee

    def hit_resting(self, qty=None):
        """Simulate the market trading into our resting limit orders: fill `qty` (or all) at the order price."""
        for o in list(self.orders.values()):
            if o["status"] in ("NEW", "PARTIALLY_FILLED") and o["type"] == "LIMIT" and not o["closePosition"]:
                remaining = o["origQty"] - o["executedQty"]
                take = min(remaining, qty) if qty else remaining
                qty_f, rp, fee = self._apply_fill(o["symbol"], o["side"], take, o["price"], o["reduceOnly"])
                self._tid += 1
                o["executedQty"] = round(o["executedQty"] + qty_f, 8)
                o["avgPrice"] = o["price"]
                o["status"] = "FILLED" if o["executedQty"] >= o["origQty"] - 1e-12 else "PARTIALLY_FILLED"
                self._emit_order(o, "TRADE", {"l": str(qty_f), "L": str(o["price"]), "n": str(fee), "N": "USDT", "t": self._tid, "rp": str(rp)})
                self._emit_account()

    async def set_leverage(self, symbol, leverage):
        self.calls["set_leverage"] += 1
        self.leverage[symbol] = int(leverage)
        return {"symbol": symbol, "leverage": int(leverage)}

    async def get_order(self, symbol, order_id):
        self.calls["get_order"] += 1
        return dict(self.orders[int(order_id)])

    async def cancel_order(self, symbol, order_id):
        self.calls["cancel_order"] += 1
        self._fail()
        o = self.orders.get(int(order_id))
        if not o or o["status"] not in ("NEW", "PARTIALLY_FILLED"):
            raise BinanceError(-2011, "Unknown order sent.")
        o["status"] = "CANCELED"
        self._emit_order(o, "CANCELED")
        return dict(o)

    async def listen_key(self):
        return "fake-listen-key"

    async def keepalive(self, key):
        self.calls["keepalive"] += 1

    async def user_stream(self, key):
        while True:
            yield await self.events.get()

    async def book_ticker(self, symbols, mark=False):
        self.calls["book_ticker"] += 1
        while True:
            for s in symbols:
                b, a = self.prices[s]
                yield {"e": "bookTicker", "s": s, "b": str(b), "a": str(a), "E": _ms()}
                if mark:
                    yield {"e": "markPriceUpdate", "s": s, "p": str((a + b) / 2), "r": "0.0001", "T": _ms() + 8 * 3600 * 1000, "E": _ms()}
            await asyncio.sleep(0.05)

    async def aclose(self):
        pass
