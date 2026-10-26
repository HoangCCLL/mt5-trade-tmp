"""Single import point for the MT5 SDK. MT5_FAKE=1 swaps in the in-memory simulator (Linux dev/tests)."""
import os

if os.environ.get("MT5_FAKE") == "1":
    from . import mt5_fake as mt5  # noqa: F401
else:
    import MetaTrader5 as mt5  # type: ignore  # noqa: F401
