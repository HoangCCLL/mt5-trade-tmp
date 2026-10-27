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

Yêu cầu: user trade đang trong phiên desktop (autologin `:0`, hoặc phiên RDP; `DISPLAY` của shell sẽ được ghi vào unit). Chạy một lần:

```bash
curl -fsSLO https://raw.githubusercontent.com/HoangCCLL/mt5-trade-tmp/main/deploy/vps/install-xubuntu.sh
GATEWAY_URL=wss://<domain> ACCOUNT_ID=demo2 AGENT_TOKEN=<token> bash install-xubuntu.sh
```

Script: cài Wine + terminal bằng `mt5ubuntu.sh` chính thức của MetaQuotes (installer GUI, đăng nhập tài khoản rồi đóng), tải `mt5agent.exe` vào `~/.mt5/drive_c/mt5agent`, ghi `agent.env`, tạo hai systemd user unit `mt5-trader`/`mt5-streamer` (`Restart=always`, `loginctl enable-linger`), chạy `timecheck` làm smoke test rồi start. Log: `journalctl --user -fu mt5-trader` hoặc `~/.mt5/drive_c/mt5agent/logs/`. Cập nhật exe: tải đè rồi `systemctl --user restart mt5-trader mt5-streamer`.

Lưu ý: terminal phải chạy trong cùng prefix và cùng `DISPLAY` với unit. Nếu dùng xrdp, `DISPLAY` đổi theo phiên; ổn định nhất là autologin `:0` + x11vnc.

## Checklist go-live một account

- [ ] `/accounts` thấy `alive:true` và `/accounts/<id>/state` có `connected:true`.
- [ ] Đặt 0.01 lot trên demo qua `POST /orders`, nhận `done`, `state` có position trong < 1 s.
- [ ] Đóng qua `POST /positions/{ticket}/close`, nhận `deal` trên SSE.
- [ ] Kéo mạng VPS 30 s: `alive` chuyển false rồi true, không lệnh đôi.
- [ ] Reboot VPS: hai task tự lên sau logon.
