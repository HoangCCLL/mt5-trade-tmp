# Deploy

## Host (BE): gateway + Redis

```bash
cp .env.example .env            # GATEWAY_API_KEY, AGENT_TOKENS, GATEWAY_BIND
docker compose up -d --build
curl http://<GATEWAY_BIND>/healthz   # {"ok":true}
curl -H "X-Api-Key: $GATEWAY_API_KEY" http://<GATEWAY_BIND>/accounts
```

- Gateway nghe HTTP thường tại `GATEWAY_BIND`. TLS/reverse proxy nằm ngoài phạm vi repo này; proxy phía trước cần cho phép WebSocket upgrade, không buffer SSE, và read timeout dài cho kết nối giữ lâu.
- Gateway chạy **một** worker uvicorn. Không scale `gateway` lên nhiều replica, registry nằm trong RAM.
- Redis chỉ nghe trong network của compose. Snapshot 60 s, mất dữ liệu khi restart là chấp nhận được (agent gửi lại state trong 30 s).
- Log: `docker compose logs -f gateway`.
- Thêm account: thêm vào `AGENT_TOKENS` trong `.env`, `docker compose up -d gateway`.

## UI

Gateway serve sẵn UI tĩnh tại `https://<domain>/ui/` (cùng origin, không cần cấu hình domain hay CORS). Nhập `GATEWAY_API_KEY` một lần, lưu trong localStorage của trình duyệt. Có: danh sách account + trạng thái, account info, positions (close, sửa SL/TP), form lệnh, tick theo symbol, log sự kiện (deal, result). Muốn host UI ở domain khác thì copy thư mục `ui/` lên bất kỳ static host nào và đổi các `fetch('/...')` thành URL gateway, kèm CORS trên gateway.

## VPS Windows: một VPS = một account = trader + streamer

Chuẩn bị một lần trên VPS:

1. Tạo user chuyên chạy trade, bật auto-logon cho user đó (Sysinternals `Autologon.exe`, mật khẩu được mã hoá trong LSA). Lý do: terminal MT5 cần phiên desktop, service session 0 không có.
2. Cài terminal portable tại `C:\mt5\<ACCOUNT_ID>\`, mở lần đầu, đăng nhập tài khoản, tick "Save password", đóng terminal. `MT5_PATH` trong `agent.env` trỏ tới `terminal64.exe` này. Agent tự mở terminal khi khởi động nếu nó chưa chạy.
3. Windows Update: đặt Active Hours phủ giờ thị trường, hoặc chấp nhận reboot cuối tuần (task tự chạy lại sau logon).
4. Firewall chỉ cần outbound 443 tới gateway. Không mở inbound.

Build exe: GitHub Actions `build-agent` chạy khi push `main` (release `latest`, ghi đè) hoặc tag `v*` (release theo version). Tải thẳng trên VPS:

```
mkdir C:\mt5agent & cd C:\mt5agent
curl.exe -L -o mt5agent.exe https://github.com/HoangCCLL/mt5-trade-tmp/releases/download/latest/mt5agent.exe
```

Build tay khi cần (máy Windows bất kỳ): `powershell -ExecutionPolicy Bypass -File deploy\vps\build.ps1` → `dist\mt5agent.exe`.

Cài trên VPS: chỉ cần copy hai file, không cần Python, không cần script.

```
C:\mt5agent\mt5agent.exe
C:\mt5agent\agent.env        # từ deploy\vps\agent.env.example, điền GATEWAY_URL, ACCOUNT_ID, AGENT_TOKEN, MT5_PATH
```

Đăng ký chạy at-logon (cmd hoặc PowerShell, user trade; `schtasks` không dính execution policy):

```
schtasks /Create /F /TN mt5-trader   /SC ONLOGON /RL LIMITED /TR "C:\mt5agent\mt5agent.exe trader"
schtasks /Create /F /TN mt5-streamer /SC ONLOGON /RL LIMITED /TR "C:\mt5agent\mt5agent.exe streamer"
schtasks /Run /TN mt5-trader & schtasks /Run /TN mt5-streamer
```

Exe tự đọc `agent.env` cạnh nó, ghi log vào `C:\mt5agent\logs\<role>.log` (xoay ở 20 MB) và tự restart sau 5 s nếu thoát, nên task không cần cấu hình restart. Chạy tay để debug: `mt5agent.exe trader` trong cửa sổ cmd.

Cập nhật: thay `mt5agent.exe`, `schtasks /End` rồi `schtasks /Run`. Gỡ: `schtasks /Delete /F /TN mt5-trader` (và streamer).

Kiểm tra sau khi cài:

```powershell
schtasks /Query /TN mt5-trader & schtasks /Query /TN mt5-streamer   # Running
# trên host:
curl -H "X-Api-Key: ..." https://$DOMAIN/accounts   # trader:true, streamer:true, alive:true
```

## VPS Xubuntu (demo/QA): terminal dưới Wine, cùng exe

Đã chạy thật 2026-10-27 với Ubuntu 24.04, **Wine 11.20 (winehq-devel) + profile Windows 11 + terminal build 6286**. Wine 10.0/11.0 hoặc profile win10 làm terminal báo "A debugger has been found running in your system". Yêu cầu: một phiên desktop đang đăng nhập (xrdp hoặc console), user có sudo.

```bash
curl -fsSLO https://raw.githubusercontent.com/HoangCCLL/mt5-trade-tmp/main/deploy/vps/install-xubuntu.sh
GATEWAY_URL=wss://<domain> ACCOUNT_ID=demo2 AGENT_TOKEN=<token> MT5_LOGIN=<login> MT5_PASSWORD="<pass>" MT5_SERVER=Broker-Demo bash install-xubuntu.sh
```

Script: cài WineHQ devel, tạo prefix `~/.mt5` (win11, tắt Mono/Gecko), cài MT5 silent từ `download.metatrader.com`, chép `ucrtbase.dll` gốc (numpy cần `crealf`), tải exe, ghi `agent.env`, tắt screensaver/lock, tạo ba systemd user unit `mt5-terminal` (ucrtbase builtin), `mt5-trader`, `mt5-streamer` (ucrtbase native) với `Restart=always` + linger, chạy `timecheck` rồi start.

Bước tay duy nhất, một lần: trong desktop, Tools > Options > Expert Advisors > tick **Allow algorithmic trading via external Python API**. Chưa tick thì lệnh trả 10027.

Cập nhật agent (không cần sudo):

```bash
systemctl --user stop mt5-trader mt5-streamer
curl -fsSL -o ~/.mt5/drive_c/mt5agent/mt5agent.exe https://github.com/HoangCCLL/mt5-trade-tmp/releases/download/latest/mt5agent.exe
systemctl --user start mt5-trader mt5-streamer
journalctl --user -u mt5-trader -u mt5-streamer --since -1m | grep connected
```

Giới hạn đo được dưới Wine (2026-10-27): trong lúc `order_send` chạy (~280 ms tới Pepperstone), event loop của trader bị đứng, nên `ack` ≈ `mt5` và lệnh thứ hai gửi cùng lúc phải chờ lệnh trước xong. Trên Windows không có hiện tượng này (`ack` ~60 ms trong khi `mt5` 400 ms). Chấp nhận cho demo/QA; production dùng Windows.

Vận hành: `journalctl --user -fu mt5-trader`, log file `~/.mt5/drive_c/mt5agent/logs/`. Cập nhật exe: tải đè rồi `systemctl --user restart mt5-trader mt5-streamer`. Dùng xrdp thì chỉ đóng cửa sổ RDP (disconnect), không Log Out; sau reboot phải RDP vào một lần để có lại display `:10`. Muốn tự lên sau reboot: autologin `:0` + x11vnc và đổi `DISPLAY=:0` trong ba unit.

## VPS Binance Futures: Docker, một VPS = một account

Không có terminal; agent nói chuyện thẳng với Binance nên chạy trên bất kỳ Linux nào có Docker. Nên đặt VPS gần AWS Tokyo (ap-northeast-1).

```bash
mkdir -p ~/binance-agent && cd ~/binance-agent
curl -fsSLO https://raw.githubusercontent.com/HoangCCLL/mt5-trade-tmp/main/deploy/binance/docker-compose.yml
curl -fsSL -o agent.env https://raw.githubusercontent.com/HoangCCLL/mt5-trade-tmp/main/deploy/binance/agent.env.example
nano agent.env            # GATEWAY_URL, ACCOUNT_ID, AGENT_TOKEN, BINANCE_API_KEY/SECRET, BINANCE_TESTNET
chmod 600 agent.env
docker compose up -d && docker compose logs -f
```

Trên gateway thêm account vào `AGENT_TOKENS` rồi `docker compose up -d --build gateway`. Cập nhật agent: `docker compose pull && docker compose up -d`. Image build tự động khi push `main` (`build-binance-image`). API key Binance: bật Futures, tắt Withdraw, giới hạn IP về IP của VPS. Spec: `docs/binance-agent-spec.md`.

## Checklist go-live một account

- [ ] `/accounts` thấy `alive:true` và `/accounts/<id>/state` có `connected:true`.
- [ ] Đặt 0.01 lot trên demo qua `POST /orders`, nhận `done`, `state` có position trong < 1 s.
- [ ] Đóng qua `POST /positions/{ticket}/close`, nhận `deal` trên SSE.
- [ ] Kéo mạng VPS 30 s: `alive` chuyển false rồi true, không lệnh đôi.
- [ ] Reboot VPS: hai task tự lên sau logon.
