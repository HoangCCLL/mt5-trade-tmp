# Trade turn MVP: Binance long/short (maker) + MT5 hedge (market)

Mục tiêu: user nhìn thị trường trên UI, bấm một nút để mở một "turn" gồm hai chân Binance (long trên account long, short
trên account short, cả hai là limit maker kiểu chase) và hai chân MT5 (market, ngược chiều, mở ngay khi chân Binance
tương ứng khớp). Đóng bằng chase (maker) hoặc Force Close (market), và tuỳ chọn tự thoát hai chân MT5 ngay khi thị
trường mở cửa đầu tuần (WMO).

Nguyên tắc chia việc: **BE/gateway ra ý định và giữ sự thật, agent trên VPS thực thi vòng lặp cần latency**
(chase, WMO), UI chỉ hiển thị và bấm.

## 1. Account set (MVP: một set, cấu hình bằng env của gateway)

```
TURN_SET={"long":"bn_long","short":"bn_short","mt5":"demo1","symbol_binance":"XAUUSDT","symbol_mt5":"XAUUSD","leverage":[1,20]}
```

- `long`, `short`: hai account Binance Futures (hai agent, hai VPS hoặc cùng VPS hai compose project), `mt5`: account MT5 ở hedge mode.
- Symbol của mỗi sàn là tên agent trả về trong `symbols.list`; gateway không map tên, không đoán.
- `leverage`: khoảng cho phép, áp cho cả hai account Binance (`account.leverage` trước khi đặt lệnh). Leverage MT5 không quản lý.
- Tối đa 20 turn đang mở.

## 2. Khối lượng: quy đổi qua đơn vị vật lý

`symbol.spec` (lệnh mới ở cả hai agent) trả cùng schema:
`{symbol, base, quote, contract_size, volume_min, volume_step, volume_max, price_step, min_notional}`.
Binance XAUUSDT: `base=XAU, contract_size=1` (qty tính bằng oz). MT5 XAUUSD: `contract_size=trade_contract_size` (Pepperstone 100 oz/lot).

User nhập khối lượng theo **oz**. Gateway tính `qty_binance = oz` (làm tròn xuống `volume_step`), `lots_mt5 = floor(oz / contract_size / step) × step`.
Từ chối nếu `base` khác nhau hoặc một bên dưới `volume_min`/`min_notional`. Kết quả quy đổi hiện trên form trước khi bấm Start.

## 3. Vòng đời một turn

```
start ─┬─ long_in  (bn_long,  BUY  chase) ──filled──► mt5_short (demo1, SELL market, ticket A)
       └─ short_in (bn_short, SELL chase) ──filled──► mt5_long  (demo1, BUY  market, ticket B)

Close  ─┬─ long_out  (bn_long,  SELL reduceOnly chase, qty đã khớp của long_in)
        ├─ short_out (bn_short, BUY  reduceOnly chase)
        └─ mt5: order.close ticket A / B (market)           ← hoặc để WMO tự làm

Force  ─┬─ chân đang chase (in hoặc out): cancel
Close   ├─ chân Binance đã khớp: order.close market reduceOnly đúng qty của turn
        └─ mt5: order.close ticket (market)            → turn = force_closed, không hoàn tác
```

Trạng thái chân: `pending → chasing → filled | canceled | failed` (Binance); `pending → sending → open → closed | failed` (MT5);
`force_closing → closed` khi Force. Trạng thái turn suy ra từ chân: `opening, open, closing, closed, force_closed, partial`
(có chân failed/canceled trong khi chân khác đã mở; user xử lý tay bằng các nút chân).

Quy tắc:
- Close một chân Binance **đang chase** = cancel chase (chưa có gì để thoát). Đã khớp một phần = cancel rồi mở out-leg cho phần đã khớp.
- Mỗi chân MT5 chỉ mở khi chân Binance cùng cặp báo `filled`; nếu MT5 trả lỗi thì chân `failed`, turn `partial`, UI nổi cảnh báo, không tự retry.
- Link lệnh ↔ turn: Binance `newClientOrderId = m{turn}-{8 hex}` (do agent sinh, bất biến, có trên mọi order/trade event; prefix `m{turn}-`
  dùng để dọn lệnh mồ côi). MT5: **không gửi comment** (broker có thể ghi đè), `magic` = số turn; link chính là `ticket` mà gateway lưu trên leg
  (`turn:{id}.legs.*.ticket`, Redis). UI tra ticket → turn/leg từ state turn, `magic` chỉ là fallback.
- Thoát Binance bằng market chỉ qua Force Close, và Force luôn tear down toàn bộ turn.

## 4. `order.chase` (Binance agent)

```json
{"symbol":"XAUUSDT","side":"buy","volume":5,"reduce_only":false,"interval_ms":500,"turn":12,"leg":"long_in"}
```

Vòng lặp, chạy trong agent, không chờ BE:
1. Đặt LIMIT, `priceMatch=QUEUE`, `timeInForce=GTX` (post-only), qty = phần còn lại.
2. Chờ tới khi **best price cùng phía đổi** (bookTicker WS) hoặc hết `interval_ms`, tuỳ cái nào trước. MVP: `interval_ms` là hằng `CHASE_INTERVAL_MS = 500` trong `gateway/turns.py`, không phải tham số form.
3. Chưa khớp hết: huỷ, đặt lại ở QUEUE mới (MVP cancel+create; nâng cấp sau: `PUT /fapi/v1/order` amend).
   GTX bị sàn từ chối vì sẽ khớp ngay (`-5022`/EXPIRED): coi như một lần dời, đặt lại ngay.
   Huỷ trả `-2011` (lệnh đã khớp trong lúc huỷ): đọc lại lệnh, cộng phần khớp.
4. Lặp **cho tới khi khớp đủ hoặc user cancel** (`chase.cancel {id}`); không có max_moves ở MVP. `-1015` (rate limit) → chờ 2 s.
5. Mỗi lần dời phát `update {id, turn, leg, order, price, best_bid, best_ask, filled, moves, elapsed_ms}`; kết thúc phát `result`
   `{status: done|canceled, filled, avg_price, moves, elapsed_ms}` kèm timing như lệnh thường.

Gateway không áp `RESULT_TIMEOUT` cho lệnh dài (`order.chase`, `order.close when=market_open`): trả 202 với id, kết quả về qua `result`.

## 5. Thoát WMO (MT5 agent)

`order.close {position: ticket, when: "market_open", max_spread_points?, deviation: 50}` → ack, giữ trong RAM; `schedule.cancel {id}` để huỷ.
Gateway lưu `sched:{acct}` trong Redis và **đẩy lại toàn bộ** mỗi lần trader (re)connect: VPS reboot cuối tuần không mất ý định.

Phát hiện mở cửa không cần lịch và không so đồng hồ VPS: agent poll `symbol_info_tick` 100 ms khi `connected`; tick mới có
`time_msc − time_msc_trước ≥ 20 h` (theo đồng hồ broker, so với chính nó) chính là tick đầu tiên sau kỳ nghỉ. Khi đó, nếu spread ≤
`max_spread_points` (mặc định không giới hạn), gửi close market, IOC, `deviation` lớn; nhận 10018/10004 thì thử lại sau 150 ms.
Vị thế không còn (SL ăn trong gap, user đóng tay) → `result position_not_found`, coi là xong. Armed giữa tuần vẫn đúng: gap ≥ 20 h
đầu tiên gặp là cuối tuần. Result mang `attempts`, spread lúc khớp, `t_first_tick`.

Checkbox **Auto Exit WMO** chỉ bật được khi `long_out` và `short_out` đã `filled` và hai chân MT5 còn `open`. Check → hai lệnh
schedule (ticket A, B). Uncheck → `schedule.cancel` cả hai. Trạng thái hiển thị: `armed` (kèm số agent xác nhận), `fired`, `done`.

## 6. Gateway

- `gateway/turns.py`: state machine + Redis (`turn:{id}` JSON, `turns` set, `cmd:{command_id} → (turn, leg)`), cache RAM, rebuild lúc start.
- Routes: `POST /turns` (start: `units, leverage`), `GET /turns`, `GET /turns/{id}`, `POST /turns/{id}/legs/{leg}/close`,
  `POST /turns/{id}/close`, `POST /turns/{id}/force-close`, `POST /turns/{id}/wmo {on: bool}`, `GET /turns/stream` (SSE),
  `GET /convert?oz=` (xem trước quy đổi), `GET /accounts/{acct}/symbols/{symbol}` (spec).
- Hook: `ws.on_trader` chuyển `result`/`update` cho `turns.on_agent_event(acct, type, payload)`; turn đổi trạng thái → publish `turns` channel.
- Lỗi và timeout của từng lệnh ghi vào chân tương ứng, không ném lên HTTP (HTTP chỉ trả về đã nhận ý định hay chưa).

## 7. UI

Hai trang cùng origin: `ui/desk.html` cho trader (mặc định), `ui/index.html` là trang kỹ thuật (lệnh lẻ, latency, tick).

Desk gồm các block độc lập (`<section data-block>` + `BLOCKS[name]()`; bố cục trong một `grid-template-areas`, đổi thứ tự ở đó):
- **Giá**: bid/ask Binance (nguồn: streamer account long, rơi về short khi im > 10 s) và MT5, tuổi tick; Δ Pair A = MT5 bid − BN bid,
  Δ Pair B = BN ask − MT5 ask (giá vào lệnh thực tế của từng cặp); funding rate + giờ funding tiếp theo (markPrice stream).
- **Start turn**: khối lượng (đơn vị vật lý), leverage Binance (slider trong `TURN_SET.leverage`). Preflight từ `/convert` + tick + state:
  qty/lot, notional, margin ước tính (Binance notional/leverage; MT5 lot × `margin_per_lot` của terminal) so với `margin_free` từng account.
  Nút Start khoá khi quy đổi không hợp lệ, thiếu trader hoặc đủ 20 turn.
- **Trade turns**: mỗi turn hai hàng A/B, bốn cột theo thời gian: Binance vào → MT5 hedge → Binance ra → MT5 đóng. Màu ô theo trạng thái:
  xám pending, xanh dương nhấp nháy chasing/sending/closing, xanh lá filled/open, xám đậm closed (ghi cách thoát maker/market/wmo), cam canceled,
  đỏ failed, tím market, xanh ngọc WMO. Nút chỉ hiện ở ô có việc: Cancel (đang chase), Close (maker) (chân Binance đã khớp), Close (MT5 đang mở).
  Nút turn: Close, Force Close (xác nhận), checkbox Auto Exit WMO (disabled kèm lý do). Turn xong gom vào "Đã đóng".
- **Log**: chỉ đọc, mỗi dòng là một event gateway đã phát (đổi trạng thái chân, result kèm latency ms hoặc thời gian chase, deal). Lọc theo turn.
- **Tab Accounts**: một block mỗi account của set: broker/server/login/leverage từ `state.account`, liveness trader/streamer, skew, used weight;
  balance/equity/uPnL/margin/free/margin level; positions (liq + leverage Binance, swap + ticket MT5, chip turn theo magic);
  lệnh chờ Binance (clientOrderId `m{turn}-…` → chip turn) hoặc lịch WMO MT5 (`/scheduled`); 20 deal gần nhất (`/accounts/{id}/deals` + SSE).
  Bỏ ở MVP: KPI strip, bảng orders toàn cục, paper/simulate, modal accounts (quyết định 2026-10-28).

## 8. Thứ tự triển khai

1. Binance agent: `account.leverage`, `symbol.spec`, `order.chase` + `chase.cancel`, fake có GTX/priceMatch/book thay đổi.
2. MT5 agent: `symbol.spec`, `order.close when=market_open`, `schedule.cancel`; gateway lưu + đẩy lại lịch.
3. Gateway: turns + routes + convert + SSE.
4. UI: turn panel.
5. Thử trên bn1 (USDC) với XRPUSDC/XRPUSD? MT5 không có XRP; set thật cần hai account Binance USDT + XAUUSDT và MT5 XAUUSD.

## 9. Giả định cần bạn xác nhận

- Hai account Binance có thể là hai sub-account cùng chủ; agent không cần biết.
- Khối lượng chase là toàn bộ qty một lần (không chia nhỏ).
- Turn đang `open` thì user vẫn được mở turn khác cùng symbol; Force Close chỉ đụng qty của turn mình.
- WMO là tự động hoá duy nhất; không có SL/TP tự động nào khác trong MVP.
