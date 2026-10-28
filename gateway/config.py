import json, os

GATEWAY_API_KEY = os.environ["GATEWAY_API_KEY"]
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
AGENT_TOKENS: dict[str, str] = json.loads(os.environ["AGENT_TOKENS"])  # {"<account_id>": "<token>"}
RESULT_TIMEOUT = float(os.environ.get("RESULT_TIMEOUT", 35))
UNSUB_GRACE = float(os.environ.get("UNSUB_GRACE", 30))

TURN_SET: dict = json.loads(os.environ.get("TURN_SET") or "{}")  # {"long","short","mt5","symbol_binance","symbol_mt5","leverage":[lo,hi]}
