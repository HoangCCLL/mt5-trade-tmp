"""Binance USDⓈ-M Futures client: signed REST (httpx) + user data stream + public bookTicker stream (websockets).
Only what the agent needs. `Fake` in fake.py implements the same surface in memory."""
import hashlib, hmac, json, time, urllib.parse
import httpx
from websockets.asyncio.client import connect

HOSTS = {False: ("https://fapi.binance.com", "wss://fstream.binance.com"),
         True: ("https://testnet.binancefuture.com", "wss://stream.binancefuture.com")}


class BinanceError(Exception):
    def __init__(self, code, msg):
        super().__init__(f"{code}: {msg}")
        self.code, self.msg = code, msg


class Binance:
    def __init__(self, key, secret, testnet=False, recv_window=5000):
        self.key, self.secret, self.testnet = key, secret.encode(), testnet
        self.rest, self.ws = HOSTS[bool(testnet)]
        self.recv_window = recv_window
        self.http = httpx.AsyncClient(base_url=self.rest, headers={"X-MBX-APIKEY": key}, timeout=10)
        self.used_weight = 0

    def _sign(self, params):
        q = urllib.parse.urlencode({**params, "recvWindow": self.recv_window, "timestamp": int(time.time() * 1000)})
        return q + "&signature=" + hmac.new(self.secret, q.encode(), hashlib.sha256).hexdigest()

    async def _call(self, method, path, params=None, signed=True):
        params = {k: v for k, v in (params or {}).items() if v is not None}
        url = f"{path}?{self._sign(params)}" if signed else (f"{path}?{urllib.parse.urlencode(params)}" if params else path)
        r = await self.http.request(method, url)
        self.used_weight = int(r.headers.get("X-MBX-USED-WEIGHT-1M", self.used_weight) or 0)
        body = r.json() if r.content else {}
        if r.status_code >= 400 or (isinstance(body, dict) and body.get("code", 0) < 0):
            raise BinanceError(body.get("code", r.status_code) if isinstance(body, dict) else r.status_code,
                               body.get("msg", r.text) if isinstance(body, dict) else r.text)
        return body

    async def exchange_info(self):
        return await self._call("GET", "/fapi/v1/exchangeInfo", signed=False)

    async def balance(self):
        return await self._call("GET", "/fapi/v3/balance")

    async def positions(self):
        return await self._call("GET", "/fapi/v3/positionRisk")

    async def open_orders(self, symbol=None):
        return await self._call("GET", "/fapi/v1/openOrders", {"symbol": symbol})

    async def place_order(self, **params):
        return await self._call("POST", "/fapi/v1/order", params)

    async def get_order(self, symbol, order_id):
        return await self._call("GET", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

    async def cancel_order(self, symbol, order_id):
        return await self._call("DELETE", "/fapi/v1/order", {"symbol": symbol, "orderId": order_id})

    async def set_leverage(self, symbol, leverage):
        return await self._call("POST", "/fapi/v1/leverage", {"symbol": symbol, "leverage": int(leverage)})

    async def listen_key(self):
        return (await self._call("POST", "/fapi/v1/listenKey", signed=False))["listenKey"]

    async def keepalive(self, key):
        await self._call("PUT", "/fapi/v1/listenKey", signed=False)

    async def user_stream(self, key):
        async with connect(f"{self.ws}/ws/{key}", ping_interval=20, ping_timeout=20) as ws:
            async for raw in ws:
                yield json.loads(raw)

    async def book_ticker(self, symbols, mark=False):
        """Best bid/ask events; with mark=True the same socket also carries markPriceUpdate (mark, funding)."""
        streams = "/".join(f"{s.lower()}@bookTicker" + (f"/{s.lower()}@markPrice@1s" if mark else "") for s in symbols)
        async with connect(f"{self.ws}/stream?streams={streams}", ping_interval=20, ping_timeout=20) as ws:
            async for raw in ws:
                yield json.loads(raw)["data"]

    async def aclose(self):
        await self.http.aclose()
