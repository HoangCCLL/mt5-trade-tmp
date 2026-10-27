import os
from .link import FATAL_CLOSE_CODES, Link, envelope, heartbeat, log  # noqa: F401  (re-exported for the MT5 agents)
from .mt5_api import mt5


def init_mt5():
    kw = {}
    if os.environ.get("MT5_PATH"):
        kw["path"] = os.environ["MT5_PATH"]
    if os.environ.get("MT5_LOGIN"):
        kw.update(login=int(os.environ["MT5_LOGIN"]), password=os.environ["MT5_PASSWORD"],
                  server=os.environ["MT5_SERVER"])
    if not mt5.initialize(**kw):
        raise SystemExit(f"mt5.initialize failed: {mt5.last_error()}")


def mt5_connected():
    ti = mt5.terminal_info()
    return bool(ti and ti.connected)
