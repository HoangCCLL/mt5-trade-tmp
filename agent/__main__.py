"""Single entrypoint: `mt5agent.exe trader|streamer` (or `python -m agent <role>`).

Reads agent.env and writes logs/<role>.log next to the exe (or the cwd when run from source).
Restarts the role forever so a scheduled task only needs to launch this once.
"""
import asyncio
import logging
import logging.handlers
import os
import sys
import time
from pathlib import Path

ROLES = ("trader", "streamer")


def base_dir():
    return Path(sys.executable).parent if getattr(sys, "frozen", False) else Path.cwd()


def load_env(path):
    if not Path(path).exists():
        return
    for line in Path(path).read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())


def main():
    role = sys.argv[1] if len(sys.argv) > 1 else ""
    if role not in ROLES + ("timecheck",):
        sys.exit(f"usage: {Path(sys.argv[0]).name} trader|streamer|timecheck [SYMBOL]")
    root = base_dir()
    load_env(root / "agent.env")
    if role == "timecheck":
        import json
        from agent.common import init_mt5
        from agent.timecheck import timecheck
        init_mt5()
        print(json.dumps(timecheck(*sys.argv[2:3]), indent=1))
        return
    (root / "logs").mkdir(exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", handlers=[
        logging.StreamHandler(),
        logging.handlers.RotatingFileHandler(root / "logs" / f"{role}.log", maxBytes=20_000_000, backupCount=2)])

    from agent import streamer, trader  # absolute: PyInstaller runs this file as a top-level script
    if sys.platform == "win32":  # default 15.6 ms scheduler tick shows up as ~16 ms thread hand-off on every order
        import ctypes
        ctypes.windll.winmm.timeBeginPeriod(1)
    url = f"{os.environ['GATEWAY_URL']}/agents/{os.environ['ACCOUNT_ID']}/{role}"
    token = os.environ["AGENT_TOKEN"]
    while True:
        try:
            if role == "trader":
                asyncio.run(trader.run(url, token, order_timeout=float(os.environ.get("ORDER_TIMEOUT", 30)),
                                       ttl=float(os.environ.get("CMD_TTL", 5))))
            else:
                asyncio.run(streamer.run(url, token))
        except KeyboardInterrupt:
            return
        except BaseException:  # SystemExit from init_mt5, closed-by-gateway, anything: restart
            logging.exception("%s exited, restarting in 5s", role)
        time.sleep(5)


if __name__ == "__main__":
    main()
