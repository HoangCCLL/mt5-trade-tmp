# MT5 Agent v3 — Spec

Trạng thái: Draft 1 · Ngày: 2026-10-26 · Thay thế: `ea-mt5-v2003005.mq5` (EA poll 10s)

## 1. Mục tiêu

- Thay EA MQL5 bằng agent Python dùng gói `MetaTrader5` chính thức.
- Bỏ polling 10 giây. Lệnh từ BE tới terminal trong < 50 ms (phần mình kiểm soát), tổng latency do broker quyết định.
- Mọi I/O chậm (order_send vài giây đến vài chục giây) không được làm gián đoạn kết nối BE ↔ agent.
- Stream trạng thái tài khoản về BE always-on; tick giá theo nhu cầu người xem.
- Một account = một Windows VPS = một cặp process `trader` + `streamer`.

Ngoài phạm vi: MT4, container hoá, HA gateway, đặt lệnh chờ phiên mở, chiến lược giao dịch.

## 2. Kiến trúc

```
VPS_i (Windows)                         BE (Linux)
┌──────────────────────┐                ┌──────────────────────────────┐
│ MT5 terminal64.exe   │                │ Gateway (1 process, asyncio) │
│   ▲ IPC      ▲ IPC   │                │  ├ registry acct → sockets   │
│ trader.py  streamer.py│                │  ├ HTTP API (web app gọi)    │
│   │ WSS      │ WSS   │                │  ├ SSE cho browser           │
└───┼──────────┼───────┘                │  └ Redis client              │
    └──────────┴────────── internet ───►│                              │
                                        └──────────┬───────────────────┘
                                                   ▼
                                        Redis: hash acct:{id}:state, pubsub acct:{id}, ticks:{id}:{sym}
```

Hướng kết nối: agent → gateway (outbound, qua NAT, không mở port trên VPS). Hai socket riêng cho `trader` và `streamer`.

## 3. Thành phần

### 3.1 `trader.py`
- Nhận command từ gateway, thực thi qua `mt5.order_send`, trả `ack` + `result`.
- Không có vòng poll. Không gọi hàm đọc trạng thái ngoài những gì cần cho lệnh.
- Thread pool 4 worker cho `mt5.*`; event loop không bao giờ gọi `mt5.*` trực tiếp.

### 3.2 `streamer.py`
- Poll `account_info`, `positions_get` mỗi 250 ms, diff, emit `state` khi đổi.
- Poll `symbol_info_tick` cho các symbol đang được subscribe, 100 ms, dedupe theo `time_msc`.
- Phát hiện deal mới qua `history_deals_get(from=last_seen)` mỗi 1 s, emit `deal`.
- Heartbeat 5 s kèm `terminal_info().connected`.
- Không gọi hàm trade.

### 3.3 Gateway
- Giữ WS của mọi agent. Registry `account_id → {trader_ws, streamer_ws}`.
- HTTP API cho web app: forward command vào `trader_ws` của đúng account, chờ `result` hoặc timeout.
- Nhận `state/tick/deal` từ `streamer`, ghi Redis, publish.
- SSE endpoint cho browser: đọc snapshot từ Redis hash rồi subscribe pubsub.
- Quản lý refcount subscribe tick theo symbol, gửi `subscribe/unsubscribe` xuống `streamer`.

### 3.4 Redis
- `acct:{id}:state` hash: `account` (json), `positions` (json), `connected`, `updated_at`.
- `acct:{id}:alive` string, `EX 15`, set mỗi heartbeat.
- pubsub `acct:{id}`: delta state, deal, result.
- pubsub `ticks:{id}:{symbol}`: tick, không lưu, gắn account vì giá mỗi broker khác nhau.

## 4. Giao thức WS

### 4.1 Kết nối
- URL: `wss://{gateway}/agents/{account_id}/{role}` với `role ∈ {trader, streamer}`.
- Header: `Authorization: Bearer {agent_token}`. Token riêng từng account, cấp từ BE, thu hồi được.
- Ping/pong do thư viện WS: `ping_interval=5`, `ping_timeout=10`.
- Reconnect: backoff 1, 2, 4, … tối đa 30 s. Outbox giữ trong RAM qua reconnect.
- Gateway nhận kết nối mới cùng `account_id/role` thì đóng kết nối cũ (kết nối mới thắng).

### 4.2 Envelope
Mọi message là một JSON object:

```json
{ "id": "uuid", "type": "string", "ts": 1761436800.123, "payload": {} }
```

- `id`: do bên gửi sinh. Command từ BE dùng `id` làm khoá idempotent.
- `ts`: epoch giây, float, giờ của bên gửi. Đồng hồ VPS phải NTP.

### 4.3 BE → trader (command)

| type | payload | ghi chú |
|---|---|---|
| `order.open` | `symbol, side ("buy"/"sell"), volume, sl?, tp?, magic, comment?, deviation?` | market order |
| `order.close` | `position (ticket), volume?` | `volume` thiếu = đóng hết |
| `order.modify` | `position (ticket), sl?, tp?` | sửa SL/TP position |
| `history.fill` | `from? (epoch)` | backfill deals, `trader` gửi `history` theo batch 50 rồi `result{count}` |
| `symbol.info` | `symbols: [..]` | bid/ask/digits/filling_mode tức thời |
| `hello` | `ts (epoch gateway)` | BE gửi ngay sau khi accept socket (cả trader lẫn streamer). Agent tính `skew = ts - now()` và kiểm tra TTL trong giờ gateway, nên đồng hồ VPS lệch không làm lệnh bị từ chối |

Mapping từ task cũ: `current_buy_order`/`current_sell_order` → `order.open`, `current_close_order` → `order.close`, `current_modify_order` → `order.modify`, `current_fill_trade` → `history.fill`, `current_symbol` → `symbol.info`. `keyDual` → `magic` (uint64) và `comment` (≤ 31 ký tự, có thể bị broker ghi đè, chỉ để đọc tay).

### 4.4 trader → BE

| type | payload |
|---|---|
| `ack` | `{ "id": command_id }` gửi ngay khi nhận, trước khi gọi MT5 |
| `history` | `{ "batch": n, "total": m, "deals": [...] }` kết quả `history.fill` |
| `result` | `{ "id", "status": "done"\|"error"\|"unknown"\|"rejected", "retcode", "comment", "order", "deal", "price", "volume", "t_recv", "t_mt5_start", "t_mt5_end" }` |
| `heartbeat` | `{ "connected": bool, "pool_busy": int, "clock_skew": float }` mỗi 5 s. `clock_skew` = giờ gateway trừ giờ VPS |

`status`:
- `done`: `retcode == TRADE_RETCODE_DONE` (10009).
- `error`: MT5 trả lỗi, `retcode` + `comment` kèm theo. Bao gồm `MARKET_CLOSED` (10018).
- `unknown`: quá `ORDER_TIMEOUT` (30 s) chưa có kết quả. Thread vẫn chạy; nếu sau đó có kết quả thật, agent gửi thêm một `result` nữa cùng `id`.
- `rejected`: agent từ chối trước khi gọi MT5. Lý do trong `comment`: `ttl_expired`, `invalid_payload`, `terminal_disconnected`.

### 4.5 BE → streamer

| type | payload |
|---|---|
| `subscribe` | `{ "symbols": [..] }` thêm vào tập đang poll |
| `unsubscribe` | `{ "symbols": [..] }` |

### 4.6 streamer → BE

| type | payload |
|---|---|
| `state` | `{ "account": {...account_info}, "positions": [...], "connected": bool }` full snapshot, chỉ gửi khi diff ≠ rỗng hoặc mỗi 30 s một lần để làm mới |
| `tick` | `{ "symbol", "bid", "ask", "time_msc" }` |
| `deal` | `{ ...deal fields từ history_deals_get }` từng deal mới |
| `heartbeat` | `{ "connected": bool, "subscribed": [..] }` mỗi 5 s |

## 5. Ngữ nghĩa thực thi lệnh

1. Gateway nhận HTTP `POST /accounts/{id}/orders`, sinh `id`, gửi command vào `trader_ws`, chờ `result` tối đa 35 s.
2. `trader` nhận: kiểm tra `now - ts <= TTL (5 s)`, nếu quá → `rejected/ttl_expired`. Kiểm tra `id` đã có trong `results` → gửi lại `result` cũ, không thực thi. Gửi `ack`.
3. Trong thread: `order_check(req)`; lỗi → `result/error` không chạm broker. OK → `order_send(req)`.
4. `req` chuẩn:
   ```python
   {"action": TRADE_ACTION_DEAL, "symbol", "volume", "type": ORDER_TYPE_BUY|SELL,
    "price": tick.ask|bid, "sl", "tp", "deviation": 20, "magic", "comment",
    "type_time": ORDER_TIME_GTC, "type_filling": <từ symbol_info cache>}
   ```
   `order.close`: thêm `"position": ticket`, side ngược lại. `order.modify`: `action = TRADE_ACTION_SLTP`.
5. `type_filling`: lấy bit đầu tiên khả dụng trong `symbol_info(s).filling_mode` theo thứ tự FOK → IOC → RETURN. Cache lúc khởi động, làm mới khi gặp `INVALID_FILL` (10030).
6. Timeout: `asyncio.wait_for(..., 30)` → `result/unknown`. Thread không bị huỷ.
7. Agent **không retry**. BE là chủ retry: gửi lại cùng `id` để lấy kết quả cũ, hoặc reconcile bằng `magic` trong `deal`/`state` từ `streamer`.
8. `results` giữ 10 000 entry gần nhất (LRU), đủ cho một ngày.

## 6. Streamer chi tiết

- **State diff:** so sánh JSON của snapshot với lần trước. Positions so theo `ticket` và các trường `volume, sl, tp, price_current, profit, swap`. Gửi full snapshot, không gửi patch, để BE và browser không cần merge.
- **Tick:** chỉ emit khi `time_msc` đổi. Không tick mới quá 10 s → hạ poll xuống 5 s cho symbol đó; có tick mới → về 100 ms. Xử lý cả cuối tuần lẫn symbol ít thanh khoản.
- **Deal:** `history_deals_get(last_seen, now + 60)` mỗi 1 s, `last_seen` = max `time` đã thấy, lọc `ticket` đã gửi trong 5 phút để tránh lặp khi giờ broker lệch.
- **Khởi động:** chờ `terminal_info().connected == True` ổn định 3 s rồi mới emit `state` đầu tiên và đặt `last_seen = now - 60`. Tránh snapshot thiếu history khi terminal vừa lên.
- **Terminal mất kết nối:** vẫn emit `heartbeat{connected:false}` và `state{connected:false}`. `terminal_info()` trả `None` → `shutdown()` + `initialize()` lại, backoff 5 s.

## 7. Gateway API

| Method | Path | Mô tả |
|---|---|---|
| `POST` | `/accounts/{id}/orders` | body = payload `order.open`; trả `result` |
| `POST` | `/accounts/{id}/orders/{ticket}/close` | `order.close` |
| `PATCH` | `/accounts/{id}/orders/{ticket}` | `order.modify` |
| `POST` | `/accounts/{id}/history/fill` | `history.fill`, trả 202 |
| `GET` | `/accounts/{id}/state` | đọc Redis hash |
| `GET` | `/accounts/{id}/stream?symbols=EURUSD,XAUUSD` | SSE: event `state`, `deal`, `tick`, `result`; tự subscribe/unsubscribe tick theo refcount |
| `GET` | `/accounts` | danh sách account, `alive`, `connected`, `last_seen` |

Lỗi chuẩn: `404` account chưa kết nối, `503` trader offline, `504` không có `result` trong 35 s (body có `id` để client hỏi lại).

Refcount tick: `symbol → số SSE client đang xem`. 0 → 1: gửi `subscribe`. về 0: chờ 30 s rồi `unsubscribe`.

## 8. Edge cases

| Tình huống | Hành vi |
|---|---|
| WS rớt giữa lúc `order_send` | Thread chạy tiếp, `result` vào outbox, gửi sau reconnect. BE thấy `ack` không `result` → chờ hoặc hỏi lại cùng `id`. |
| BE gửi trùng `id` | Agent trả `result` cũ, không đặt lệnh. |
| Command tới muộn (VPS mất mạng 1 phút) | `rejected/ttl_expired`. BE quyết định gửi lại với `id` mới hay bỏ. |
| Cuối tuần | `order.*` → `error/10018` ngay. `streamer` tick backoff 5 s, state không đổi, heartbeat vẫn chạy. |
| Broker bảo trì cuối tuần | `heartbeat.connected=false`. Dashboard phân biệt "agent sống, broker off". |
| VPS reboot (Windows Update) | Hai process là Windows service auto-start, terminal auto-login. `initialize(path=...)` tự mở terminal nếu chưa chạy. |
| Mở cửa thứ Hai, gap hit SL hàng loạt | Một `state` chứa nhiều thay đổi, nhiều `deal` liên tiếp. BE không giả định một diff = một thay đổi. |
| Thread pool đầy (4 lệnh đang treo) | Lệnh thứ 5 xếp hàng trong pool, vẫn `ack`. `heartbeat.pool_busy` để BE biết. |
| `INVALID_FILL` | Làm mới cache filling mode, trả `error`; BE gửi lại nếu muốn. |

## 9. Triển khai trên VPS

- Windows Server/10/11, Python 3.11 x64, `pip install MetaTrader5 websockets`.
- Terminal cài portable tại `C:\mt5\{account}`, auto-login lưu trong `config\common.ini`.
- Hai service qua NSSM: `mt5-trader-{account}`, `mt5-streamer-{account}`, restart on failure, log ra file xoay vòng.
- Config qua env: `GATEWAY_URL, ACCOUNT_ID, AGENT_TOKEN, MT5_PATH, MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, ORDER_TIMEOUT=30, CMD_TTL=5`.
- NTP bật. Lệch giờ > 2 s sẽ làm TTL từ chối sai.
- Tắt Windows Update tự reboot trong giờ thị trường (Active Hours) hoặc chấp nhận reboot cuối tuần.

## 10. Quan sát

- Mỗi `result` mang `t_recv, t_mt5_start, t_mt5_end`; gateway thêm `t_sent, t_result`. Tính được: mạng xuống, hàng đợi pool, broker, mạng lên.
- Gateway đếm: command/s, p50/p95/p99 end-to-end, tỉ lệ `error/unknown/rejected`, số agent alive.
- Log có cấu trúc (JSON một dòng), gắn `account_id` và `id` command.

## 11. Bảo mật

- Chỉ WSS. Không còn HTTP thường như API cũ.
- Token per account, thu hồi được, không phải `login:password` của broker.
- Gateway kiểm tra `account_id` trong URL khớp với token.
- Agent không expose port nào.

## 12. Tiêu chí hoàn thành MVP

1. Demo account: `POST /orders` → `result/done` trung bình < 500 ms end-to-end, phần không phải broker < 20 ms.
2. Bắn 3 lệnh đồng thời trong khi `streamer` poll liên tục: không crash, không `None` bất thường, `state` vẫn cập nhật trong lúc lệnh treo.
3. Kéo mạng VPS 30 s giữa lúc đặt lệnh: `result` về sau khi reconnect, không có lệnh đôi khi BE gửi lại.
4. Mở SSE trên browser: thấy snapshot ngay, tick trong < 300 ms sau subscribe.
5. Cuối tuần: agent sống, `connected` phản ánh đúng, `order.open` trả 10018.

## 13. Phase 2 (không làm bây giờ)

- EA mini `OnTradeTransaction` → socket local → `streamer`, nếu đo thấy poll 250 ms không đủ.
- Redis Streams cho replay equity curve.
- HA gateway, routing command qua channel nội bộ.
- Protobuf/gRPC thay JSON/WS nếu cần schema cứng.

## Đồng hồ: ba nguồn thời gian khác nhau

| Nguồn | Dùng cho | Không được dùng cho |
|---|---|---|
| Gateway (`ts` trong envelope, `hello`) | TTL lệnh, log latency | |
| VPS (`time.time()`) | khoảng cách tương đối (backoff tick, prune cache) | so với `ts` gateway hoặc timestamp MT5 |
| Broker server (tick `time`, deal `time`, tham số `history_deals_get`) | dedupe, hiển thị | so với giờ VPS/UTC. Đo được +3 h trên Pepperstone-Demo dù tài liệu nói "UTC" |

`mt5agent.exe timecheck [SYMBOL]` in các offset đo thực tế; chạy ngay sau một lệnh để deal mới nhất còn tươi.
