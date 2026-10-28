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
- `magic` MT5 = số thứ tự turn, `comment` = `t{turn}-{leg}`; Binance `newClientOrderId` = `t{turn}-{leg}-{8 hex}`; đủ để đối chiếu từ deal/user stream.
- Thoát Binance bằng market chỉ qua Force Close, và Force luôn tear down toàn bộ turn.

## 4. `order.chase` (Binance agent)

```json
{"symbol":"XAUUSDT","side":"buy","volume":5,"reduce_only":false,"interval_ms":500,"turn":12,"leg":"long_in"}
```

Vòng lặp, chạy trong agent, không chờ BE:
1. Đặt LIMIT, `priceMatch=QUEUE`, `timeInForce=GTX` (post-only), qty = phần còn lại.
2. Chờ tới khi **best price cùng phía đổi** (bookTicker WS) hoặc hết `interval_ms`, tuỳ cái nào trước.
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
- Routes: `POST /turns` (start: `oz, leverage, interval_ms`), `GET /turns`, `GET /turns/{id}`, `POST /turns/{id}/legs/{leg}/close`,
  `POST /turns/{id}/close`, `POST /turns/{id}/force-close`, `POST /turns/{id}/wmo {on: bool}`, `GET /turns/stream` (SSE),
  `GET /convert?oz=` (xem trước quy đổi), `GET /accounts/{acct}/symbols/{symbol}` (spec).
- Hook: `ws.on_trader` chuyển `result`/`update` cho `turns.on_agent_event(acct, type, payload)`; turn đổi trạng thái → publish `turns` channel.
- Lỗi và timeout của từng lệnh ghi vào chân tương ứng, không ném lên HTTP (HTTP chỉ trả về đã nhận ý định hay chưa).

## 7. UI (turn panel)

- Form Start: leverage (slider trong khoảng cấu hình), oz, interval; hiện ngay `qty Binance` và `lots MT5` sau quy đổi, kèm cảnh báo dưới mức tối thiểu.
- Mỗi turn một card, hai hàng cặp: `bn_long BUY ⟶ mt5 SELL` và `bn_short SELL ⟶ mt5 BUY`. Mỗi chân là một chip trạng thái:
  xám pending, xanh dương nhấp nháy **chasing** (giá đang đặt, best bid/ask, số lần dời, thời gian), xanh lá filled/open (giá, qty, ticket),
  cam canceled, đỏ failed, tím **force/market**. Ký hiệu rõ: Binance maker (chase) và Force market là hai hành động khác nhau, nút màu khác.
- Nút chân: Close (chase out / close ticket), Cancel (chỉ khi đang chase). Nút turn: Close all, Force Close (hỏi xác nhận), checkbox WMO (disabled kèm lý do khi chưa đủ điều kiện).
- Cột PnL: Binance `unRealizedProfit` của qty turn, MT5 `profit` theo ticket, tổng turn. Timeline mini cho mỗi chase (giá đặt theo thời gian).
- View sàn (ngoài turn): giữ các card Account/Positions/Orders/Deals hiện có cho từng account trong set, đủ để đối chiếu với web Binance và terminal MT5.

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
