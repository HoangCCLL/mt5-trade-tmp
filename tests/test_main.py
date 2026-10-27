import os

from agent.__main__ import load_env, ROLES


def test_load_env_parses_file_without_overriding_existing(tmp_path, monkeypatch):
    f = tmp_path / "agent.env"
    f.write_text("# comment\nACCOUNT_ID=demo1\nMT5_PATH=C:\\Program Files\\MetaTrader 5\\terminal64.exe\n\nGATEWAY_URL = ws://x\n")
    monkeypatch.setenv("ACCOUNT_ID", "keep")
    monkeypatch.delenv("MT5_PATH", raising=False)
    monkeypatch.delenv("GATEWAY_URL", raising=False)
    load_env(f)
    assert os.environ["ACCOUNT_ID"] == "keep"
    assert os.environ["MT5_PATH"] == "C:\\Program Files\\MetaTrader 5\\terminal64.exe"
    assert os.environ["GATEWAY_URL"] == "ws://x"


def test_load_env_missing_file_is_noop(tmp_path):
    load_env(tmp_path / "nope.env")


def test_roles():
    assert set(ROLES) == {"trader", "streamer"}


def test_timecheck_measures_broker_clock_offset(monkeypatch):
    monkeypatch.setenv("MT5_FAKE", "1")
    from agent.mt5_api import mt5
    from agent.timecheck import timecheck
    mt5.reset()
    mt5.clock_offset = 3 * 3600
    mt5.order_send({"action": mt5.TRADE_ACTION_DEAL, "symbol": "EURUSD", "volume": 0.1, "type": mt5.ORDER_TYPE_BUY,
                    "price": 1.1, "magic": 1})
    r = timecheck("EURUSD")
    assert abs(r["tick_offset_s"] - 10800) < 5
    assert abs(r["deal_offset_s"] - 10800) < 5
    assert r["deal_in_naive_60s_window"] is False and r["deal_in_int_1day_window"] is True
