#!/usr/bin/env bash
# One-shot setup on a clean Xubuntu VPS: Wine + MT5 terminal (official MetaQuotes script), the agent exe,
# and two systemd user services. Run as the trade user INSIDE the graphical session (the MT5 installer is a GUI):
#   GATEWAY_URL=wss://host ACCOUNT_ID=demo2 AGENT_TOKEN=... bash install-xubuntu.sh
set -euo pipefail
: "${GATEWAY_URL:?}" "${ACCOUNT_ID:?}" "${AGENT_TOKEN:?}"
EXE_URL=${EXE_URL:-https://github.com/HoangCCLL/mt5-trade-tmp/releases/download/latest/mt5agent.exe}
PREFIX=${WINEPREFIX:-$HOME/.mt5}
if [ -z "${DISPLAY:-}" ]; then  # run over SSH: borrow the display of the user's running desktop session (xrdp = :10, console = :0)
    sp=$(pgrep -u "$USER" -x xfce4-session | head -1)
    DISPLAY=$(tr '\0' '\n' < "/proc/${sp:-0}/environ" 2>/dev/null | sed -n 's/^DISPLAY=//p')
    [ -n "$DISPLAY" ] || { echo "no desktop session found; log in via RDP/console first or pass DISPLAY="; exit 1; }
fi
export DISPLAY XAUTHORITY=${XAUTHORITY:-$HOME/.Xauthority}
echo ">> using DISPLAY=$DISPLAY"
TERMINAL="$PREFIX/drive_c/Program Files/MetaTrader 5/terminal64.exe"

sudo apt-get update -qq && sudo apt-get install -y -qq curl
if [ ! -f "$TERMINAL" ]; then
    echo ">> installing Wine + MetaTrader 5 (GUI installer will open: click through, then log in and close it)"
    curl -fsSL https://download.mql5.com/cdn/web/metaquotes.software.corp/mt5/mt5ubuntu.sh -o /tmp/mt5ubuntu.sh
    bash /tmp/mt5ubuntu.sh
fi
[ -f "$TERMINAL" ] || { echo "terminal64.exe not found at $TERMINAL"; exit 1; }

AGENT="$PREFIX/drive_c/mt5agent"
mkdir -p "$AGENT"
curl -fsSL "$EXE_URL" -o "$AGENT/mt5agent.exe"
cat > "$AGENT/agent.env" <<ENV
GATEWAY_URL=$GATEWAY_URL
ACCOUNT_ID=$ACCOUNT_ID
AGENT_TOKEN=$AGENT_TOKEN
MT5_PATH=C:\\Program Files\\MetaTrader 5\\terminal64.exe
ENV

mkdir -p ~/.config/systemd/user
for role in trader streamer; do
cat > ~/.config/systemd/user/mt5-$role.service <<UNIT
[Unit]
Description=MT5 agent $role
[Service]
Environment=WINEPREFIX=$PREFIX WINEDEBUG=-all DISPLAY=$DISPLAY XAUTHORITY=$XAUTHORITY
ExecStart=/usr/bin/wine C:\\\\mt5agent\\\\mt5agent.exe $role
Restart=always
RestartSec=5
[Install]
WantedBy=default.target
UNIT
done
systemctl --user daemon-reload
sudo loginctl enable-linger "$USER"   # user services keep running without an open login shell

echo ">> smoke test (exe under Wine + SDK sees the terminal + clock offsets):"
WINEPREFIX=$PREFIX WINEDEBUG=-all wine "C:\\mt5agent\\mt5agent.exe" timecheck
systemctl --user enable --now mt5-trader mt5-streamer
systemctl --user --no-pager status mt5-trader mt5-streamer | grep -E "mt5-|Active"
echo ">> logs: journalctl --user -fu mt5-trader   |  $AGENT/logs/"
