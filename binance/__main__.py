"""`python -m binance trader|streamer`. Credentials and gateway settings from the environment."""
import asyncio, logging, os, sys, time

ROLES = ("trader", "streamer")


def make_exchange():
    if os.environ.get("BINANCE_FAKE") == "1":
        from .fake import Fake
        return Fake()
    from .exchange import Binance
    return Binance(os.environ["BINANCE_API_KEY"], os.environ["BINANCE_API_SECRET"], testnet=os.environ.get("BINANCE_TESTNET") == "1")


def main():
    role = sys.argv[1] if len(sys.argv) > 1 else ""
    if role not in ROLES:
        sys.exit("usage: python -m binance trader|streamer")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    from . import streamer, trader
    url = f"{os.environ['GATEWAY_URL']}/agents/{os.environ['ACCOUNT_ID']}/{role}"
    token = os.environ["AGENT_TOKEN"]
    while True:
        try:
            ex = make_exchange()
            if role == "trader":
                asyncio.run(trader.run(url, token, ex, order_timeout=float(os.environ.get("ORDER_TIMEOUT", 30)),
                                       ttl=float(os.environ.get("CMD_TTL", 5))))
            else:
                asyncio.run(streamer.run(url, token, ex))
        except KeyboardInterrupt:
            return
        except BaseException:
            logging.exception("%s exited, restarting in 5s", role)
        time.sleep(5)


if __name__ == "__main__":
    main()
