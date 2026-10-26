# Deploy

## Host (BE): gateway + Redis + Caddy

```bash
cp .env.example .env            # điền DOMAIN, GATEWAY_API_KEY, AGENT_TOKENS
docker compose up -d --build
curl https://$DOMAIN/healthz    # {"ok":true}
curl -H "X-Api-Key: $GATEWAY_API_KEY" https://$DOMAIN/accounts
```

- `DOMAIN` trỏ DNS về host, mở port 80/443. Caddy tự lấy cert Let's Encrypt.
- Không có domain (LAN/test): `DOMAIN=:80`, agent dùng `GATEWAY_URL=ws://<host-ip>`.
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

Cài agent (PowerShell, user trade, từ thư mục repo đã copy sang VPS):

```powershell
Copy-Item deploy\vps\agent.env.example deploy\vps\agent.env   # điền GATEWAY_URL, ACCOUNT_ID, AGENT_TOKEN, MT5_*
.\deploy\vps\install.ps1
Get-Content C:\mt5agent\logs\trader.log -Wait
```

Script làm gì: cài Python 3.11 qua winget nếu thiếu, `pip install MetaTrader5 websockets`, copy `agent/` + `run-agent.ps1` + `agent.env` vào `C:\mt5agent`, đăng ký hai scheduled task `mt5-trader`, `mt5-streamer` chạy at-logon trong phiên tương tác với restart vô hạn, resync NTP, start luôn.

Cập nhật code: chạy lại `install.ps1`. Gỡ: `install.ps1 -Uninstall`.

Kiểm tra sau khi cài:

```powershell
Get-ScheduledTask mt5-* | ft TaskName, State        # Running
# trên host:
curl -H "X-Api-Key: ..." https://$DOMAIN/accounts   # trader:true, streamer:true, alive:true
```

## Checklist go-live một account

- [ ] `/accounts` thấy `alive:true` và `/accounts/<id>/state` có `connected:true`.
- [ ] Đặt 0.01 lot trên demo qua `POST /orders`, nhận `done`, `state` có position trong < 1 s.
- [ ] Đóng qua `POST /positions/{ticket}/close`, nhận `deal` trên SSE.
- [ ] Kéo mạng VPS 30 s: `alive` chuyển false rồi true, không lệnh đôi.
- [ ] Reboot VPS: hai task tự lên sau logon.
