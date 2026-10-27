# Integration test — các bước theo thứ tự

Mỗi bước có điều kiện đạt. Không qua bước trước thì không làm bước sau. Dùng tài khoản **demo** cho đến bước 8.

Biến dùng xuyên suốt:
```bash
export GW=https://gateway.example.com KEY=<GATEWAY_API_KEY> A=<ACCOUNT_ID>
alias gw='curl -s -H "X-Api-Key: $KEY" -H "Content-Type: application/json"'
```

## 0. Local (đã xong)
- `uv run pytest` → 24 passed.
- `docker compose up` + agent giả: order `done`, SSE có tick.

## 1. Host staging: TLS, token, firewall

**Dev chưa có domain:** host `.env` đặt `DOMAIN=:80`, VPS `GATEWAY_URL=ws://<ip-host>`, mở port 80 inbound. Mọi lệnh bên dưới thay `https://` → `http://`, `wss://` → `ws://`. Host sau NAT thì cài Tailscale hai đầu và dùng IP `100.x.x.x`. Bỏ qua phần kiểm tra cert ở bước này.

Trên host: `.env` thật (`DOMAIN`, `GATEWAY_API_KEY`, `AGENT_TOKENS`), `docker compose up -d --build`.
Từ một máy Linux bất kỳ, agent giả qua internet:
```bash
MT5_FAKE=1 GATEWAY_URL=wss://$DOMAIN ACCOUNT_ID=$A AGENT_TOKEN=<token> uv run python -m agent.trader &
MT5_FAKE=1 GATEWAY_URL=wss://$DOMAIN ACCOUNT_ID=$A AGENT_TOKEN=<token> uv run python -m agent.streamer &
gw $GW/healthz; gw $GW/accounts
```
Đạt: `healthz` ok qua HTTPS, cert hợp lệ, `/accounts` thấy `trader:true streamer:true alive:true`. Sai token → agent log `closed with 4401` và tự thoát, không reconnect loop.

## 2. VPS: agent chạy tay với MT5 thật
Chuẩn bị VPS theo `deploy/deploy.md` (user auto-logon, terminal portable đã login demo). Chưa cài task, chạy foreground để nhìn log:
```powershell
# PowerShell, user trade, trong thư mục repo
Get-Content deploy\vps\agent.env | % { if ($_ -match '^([^#=]+)=(.*)$') { Set-Item "env:$($Matches[1].Trim())" $Matches[2].Trim() } }
py -3.11 -m agent.streamer      # cửa sổ 1
py -3.11 -m agent.trader        # cửa sổ 2
```
Đạt: log `connected wss://...`; trên host `gw $GW/accounts/$A/state` trả `connected:true`, `account.balance` đúng số dư demo, `positions` khớp terminal.
Nếu `mt5.initialize failed`: kiểm tra `MT5_PATH`, terminal đã login và lưu mật khẩu, không có terminal khác đang mở cùng thư mục.

## 3. Trade path cơ bản trên demo
```bash
gw -d '{"symbol":"EURUSD","side":"buy","volume":0.01,"magic":1001}' $GW/accounts/$A/orders
# ghi lại order (= position ticket) từ response
gw -X PATCH -d '{"sl":<giá dưới 50 pip>,"tp":<giá trên 50 pip>}' $GW/accounts/$A/positions/<ticket>
gw -X POST $GW/accounts/$A/positions/<ticket>/close
```
Song song mở `curl -N -H "X-Api-Key: $KEY" "$GW/accounts/$A/stream?symbols=EURUSD"` ở terminal khác.
Đạt:
- `status:done`, `retcode:10009`; lệnh hiện trong tab Trade của terminal với magic 1001.
- SSE: `state` có position trong < 1 s sau `done`; `deal` entry IN; sau close có `deal` entry OUT và `state` không còn position.
- Modify: terminal hiện SL/TP mới.
- Latency: trong `result` có `t_recv, t_mt5_start, t_mt5_end`. Ghi `t_mt5_end - t_mt5_start` (broker) và tổng thời gian curl. Mục tiêu: phần không phải broker < 50 ms.
- Nếu `retcode 10030 INVALID_FILL`: gửi lại một lần, agent đã làm mới cache filling mode. Vẫn lỗi → báo lại để xử lý theo broker.

## 4. Đồng thời + thread-safety SDK (bước quan trọng nhất)
Trong lúc SSE đang mở và tick đang chạy:
```bash
for i in 1 2 3; do gw -d '{"symbol":"EURUSD","side":"buy","volume":0.01,"magic":200'$i'}' $GW/accounts/$A/orders & done; wait
```
Đạt: cả 3 `done`, 3 position trong terminal, `state` vẫn cập nhật trong lúc lệnh đang chạy (tick không ngắt quãng > 1 s), log trader/streamer không có traceback, không có `None` bất thường. Lặp 5 lần. Đóng hết position sau khi xong.
Nếu streamer crash hoặc trả `None` liên tục trong lúc trader gửi lệnh: đây là giới hạn của gói MetaTrader5 khi hai process cùng IPC. Ghi lại log và dừng, cần quyết định lại kiến trúc (một process duy nhất, hoặc streamer tạm dừng poll khi trader đang bận).

## 5. Tình huống lỗi
| Làm | Đạt |
|---|---|
| Ctrl+C trader, `gw ... /orders` | 503 `trader offline`; bật lại → `/accounts` alive trong < 10 s |
| Đặt lệnh với `-H "Idempotency-Key: t1"` hai lần | cùng `order`, terminal chỉ có một position |
| Tắt adapter mạng VPS 30 s rồi bật (`Disable-NetAdapter`/`Enable-NetAdapter`) | `alive:false` sau ≤ 15 s, rồi `true`; agent log reconnect với backoff; không lệnh đôi |
| Tắt mạng **ngay sau** khi gửi lệnh, bật lại sau 20 s | HTTP nhận 504 với `id`; sau reconnect SSE nhận `result` cùng `id`; gửi lại cùng `Idempotency-Key` → trả kết quả cũ, không đặt thêm |
| Tạm đặt `ORDER_TIMEOUT=0.05` trong `agent.env`, restart trader, đặt lệnh | 504 rồi SSE nhận `result unknown` và sau đó `result done` cùng `id`. Trả lại 30 sau khi xong |
| Gửi lệnh với `volume: 1000` | `status:error`, retcode từ `order_check`, không chạm broker (`t_mt5_end - t_mt5_start` ≈ 0) |
| Đóng terminal MT5 bằng tay | `state.connected:false` trong ≤ 1 s, `heartbeat` vẫn chạy; mở lại terminal → `connected:true` |

## 6. Cài scheduled task + reboot
```
schtasks /Create /F /TN mt5-trader   /SC ONLOGON /RL LIMITED /TR "C:\mt5agent\mt5agent.exe trader"
schtasks /Create /F /TN mt5-streamer /SC ONLOGON /RL LIMITED /TR "C:\mt5agent\mt5agent.exe streamer"
shutdown /r /t 0
```
Sau khi VPS lên và auto-logon: `/accounts` alive trong < 2 phút, log `C:\mt5agent\logs\*.log` có dòng kết nối. Kill `mt5agent.exe` bằng Task Manager → task không tự chạy lại (chỉ at-logon), nên kiểm tra thay bằng: rút mạng 30 s → exe tự reconnect; tắt terminal MT5 → exe log lỗi, tự thử lại sau 5 s.

## 7. Cuối tuần
Thứ Bảy: `gw ... /orders` → `status:error retcode:10018`. `/state` `connected` phản ánh đúng việc broker bảo trì. Tick: SSE không spam, log streamer không đầy.
Thứ Hai sau mở cửa: `alive:true` mà không cần can thiệp, không có lệnh nào tự chạy từ thứ Sáu (kiểm tra tab History của terminal).

## 8. Soak 24h trên demo
Chạy liên tục một ngày giao dịch, đặt/đóng vài lệnh rải rác. Kiểm tra cuối ngày:
- Log size hai agent, số lần reconnect (`grep -c "ws closed"`).
- RAM `python.exe` không tăng liên tục (`results` LRU 10k, `seen` deals 5 phút).
- `docker stats` gateway và redis ổn định; `redis-cli info memory`.
- Không có `unknown` nào không được theo sau bởi `done`/`error`.

Qua hết 8 bước → chuyển sang tài khoản thật với 0.01 lot, lặp lại bước 3 một lần.
