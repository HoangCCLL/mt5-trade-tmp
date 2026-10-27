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
