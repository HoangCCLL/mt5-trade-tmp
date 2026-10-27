# Chạy MVP (gateway + agent) trên Linux với MT5 giả

```bash
docker run -d --name mt5-redis -p 6379:6379 redis:7-alpine
uv sync
uv run pytest                      # 23 test end-to-end, cần Redis

# terminal 1: gateway
export GATEWAY_API_KEY=k AGENT_TOKENS='{"A1":"tokA1"}' REDIS_URL=redis://localhost:6379/0
uv run uvicorn gateway.main:app --port 8765

# terminal 2 + 3: agent với MT5 giả
export MT5_FAKE=1 GATEWAY_URL=ws://127.0.0.1:8765 ACCOUNT_ID=A1 AGENT_TOKEN=tokA1
uv run python -m agent.trader
uv run python -m agent.streamer

# thử
curl -H 'X-Api-Key: k' localhost:8765/accounts
curl -H 'X-Api-Key: k' -H 'Content-Type: application/json' \
     -d '{"symbol":"EURUSD","side":"buy","volume":0.1,"magic":42}' localhost:8765/accounts/A1/orders
curl -N -H 'X-Api-Key: k' 'localhost:8765/accounts/A1/stream?symbols=EURUSD'
```

## Trên Windows VPS (MT5 thật)

```powershell
py -3.11 -m pip install MetaTrader5 websockets
$env:GATEWAY_URL="wss://gateway.example.com"; $env:ACCOUNT_ID="A1"; $env:AGENT_TOKEN="..."
$env:MT5_PATH="C:\mt5\A1\terminal64.exe"; $env:MT5_LOGIN="..."; $env:MT5_PASSWORD="..."; $env:MT5_SERVER="..."
py -3.11 -m agent.trader
py -3.11 -m agent.streamer
```
Chạy dưới NSSM/Task Scheduler cho auto-start. Chỉ cần copy thư mục `agent/`.

## Cấu trúc

```
gateway/   FastAPI: WS cho agent, HTTP + SSE cho web/browser, Redis state + pubsub
agent/     trader.py (lệnh), streamer.py (state/tick/deal), common.py (WS link), mt5_fake.py (simulator)
tests/     conftest.py (server fixture + FakeAgent), test_gateway.py, test_agent.py
deploy/    vps/build.ps1 (PyInstaller -> mt5agent.exe), vps/agent.env.example
docs/      mt5-agent-spec.md, gateway-spec.md, deploy.md
```
