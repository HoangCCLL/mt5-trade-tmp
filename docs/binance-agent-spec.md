# Binance Futures agent (USDⓈ-M)

Cùng gateway, cùng protocol với MT5 (`hello / ack / result / state / tick / deal / heartbeat`, timing `d_pool / d_mt5`).
Khác MT5: không có terminal, nói chuyện thẳng với Binance qua REST ký HMAC + WebSocket, nên agent là asyncio thuần,
không thread pool. Vẫn hai process `trader` và `streamer`, hai container từ một image, một VPS = một account.

## Cấu hình (env)

| Biến | Bắt buộc | Ghi chú |
|---|---|---|
| `GATEWAY_URL`, `ACCOUNT_ID`, `AGENT_TOKEN` | ✓ | như MT5 |
| `BINANCE_API_KEY`, `BINANCE_API_SECRET` | ✓ | HMAC key của account futures |
| `BINANCE_TESTNET` | | `1` → `testnet.binancefuture.com`; mặc định prod `fapi.binance.com` |
| `BINANCE_FAKE` | | `1` → sàn giả trong RAM (test) |
| `ORDER_TIMEOUT`, `CMD_TTL` | | như MT5 |

## Lệnh (trader)

| type | payload | Binance |
|---|---|---|
| `order.open` | `symbol, side (buy/sell), volume (qty), type? (market\|limit), price?, sl?, tp?, reduce_only?, magic?, comment?` | `POST /fapi/v1/order` MARKET hoặc LIMIT GTC; `sl`/`tp` → thêm `STOP_MARKET` / `TAKE_PROFIT_MARKET` với `closePosition=true` |
| `order.close` | `position (= symbol), volume?` | MARKET ngược chiều, `reduceOnly=true`, qty = \|positionAmt\| hoặc `volume` |
| `order.modify` | `position (= symbol), sl?, tp?` | huỷ algo cũ cùng loại, đặt lại `STOP_MARKET` / `TAKE_PROFIT_MARKET closePosition` |
| `order.cancel` | `symbol, order` | `DELETE /fapi/v1/order` |
| `symbols.list` | | `exchangeInfo` symbols `TRADING`, cache 1 h |

`magic` → `newClientOrderId = m{magic}-{8 hex}` để đọc lại từ user stream. `comment` không có trên Binance, trả lại nguyên trong result.
`result`: `status done|error|unknown|rejected`, `retcode` = Binance `code` khi lỗi (0 khi ok), `order` = orderId,
`price` = avgPrice (market) hoặc price (limit), `volume` = executedQty, cùng `t_recv, t_skew, t_sent, d_pool, d_mt5`.
Mode vị thế: one-way (`positionSide=BOTH`). Hedge mode không hỗ trợ ở MVP.

## State (streamer)

```json
{"account": {"login": "<api key 6 ký tự đầu>", "server": "binance-futures[-testnet]", "currency": "USDT",
             "balance": wallet, "equity": wallet + uPnL, "margin": initialMargin, "margin_free": availableBalance,
             "profit": uPnL, "used_weight": X-MBX-USED-WEIGHT-1M của REST call cuối},
 "positions": [{"ticket": "BTCUSDT", "symbol": "BTCUSDT", "type": 0|1, "volume": |amt|, "price_open": entry,
                "price_current": mark, "profit": uPnL, "sl": stop của STOP_MARKET closePosition hoặc null,
                "tp": ..., "magic": 0, "leverage": n, "liquidation": liquidationPrice hoặc null (chỉ cập nhật ở reconcile 30 s)}],
 "orders": [{"order": id, "symbol", "side", "type", "price", "stop": stopPrice, "qty", "filled", "status", "client": clientOrderId}],
 "funding": {"BTCUSDT": {"rate": r, "next": T ms, "mark": p}},   // chỉ symbol đang được subscribe; từ stream markPrice@1s cùng socket với bookTicker
 "connected": true}
```
`ticket` của position là symbol (Binance không có ticket), nên route `/positions/{ticket}/close` nhận chuỗi.
Nguồn: REST lúc khởi động và mỗi 30 s (`/fapi/v3/balance`, `/fapi/v3/positionRisk`, `/fapi/v1/openOrders`), giữa hai lần
cập nhật bằng user stream `ACCOUNT_UPDATE` (balance, position) và `ORDER_TRADE_UPDATE` (orders). Phát `state` khi khác.

`deal`: mỗi `ORDER_TRADE_UPDATE` có `x == "TRADE"` → `{ticket: t, order: i, symbol, side, entry: 0|1 (mở/đóng theo reduceOnly hoặc dấu vị thế),
volume: l, price: L, profit: rp, commission: n, time_msc: T, magic}`.

`tick`: `subscribe {symbols}` → stream công khai `<symbol>@bookTicker` → `{symbol, bid, ask, time_msc}`; `unsubscribe` đóng stream.

## Kết nối Binance

- listenKey: `POST /fapi/v1/listenKey`, gia hạn `PUT` mỗi 30 phút, user stream `wss://fstream.binance.com/ws/<key>`; rớt thì tạo key mới, nối lại, reconcile REST ngay.
- Rate limit: tôn trọng header `X-MBX-USED-WEIGHT-1M` và `X-MBX-ORDER-COUNT-10S`; 429/418 → `retcode` tương ứng, không retry lệnh (idempotency do gateway).
- Đồng hồ: `recvWindow=5000`; lệch quá thì Binance trả -1021, agent log và báo trong heartbeat `clock_skew`.

## Triển khai

Image `ghcr.io/hoangccll/mt5-trade-tmp/binance-agent` (CI push mỗi lần `main`), compose trong `deploy/binance/` với hai
service `trader`, `streamer`, một `agent.env`. VPS Linux bất kỳ có Docker; nên đặt gần AWS Tokyo (ap-northeast-1) nơi Binance Futures matching engine chạy.
