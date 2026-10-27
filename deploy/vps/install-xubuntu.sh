#!/usr/bin/env bash
# One-shot setup on a clean Xubuntu VPS: Wine + MT5 terminal (official MetaQuotes script), the agent exe,
# and two systemd user services. Run as the trade user INSIDE the graphical session (the MT5 installer is a GUI):
#   GATEWAY_URL=wss://host ACCOUNT_ID=demo2 AGENT_TOKEN=... MT5_LOGIN=123 MT5_PASSWORD=... MT5_SERVER=Broker-Demo bash install-xubuntu.sh
# MT5_* make the agent log the terminal in itself (headless); without them log in once in the terminal GUI and save the password.
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

# Same steps as MetaQuotes' mt5ubuntu.sh, but non-interactive and fetching the installer from download.metatrader.com
# (download.mql5.com served an expired certificate on 2026-10-27). WebView2 is skipped: only the built-in browser needs it.
SETUP_URL=${SETUP_URL:-https://download.metatrader.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe}
# Wine 11.0 makes mt5setup.exe abort with "A debugger has been found running in your system"
# (https://www.mql5.com/en/forum/503128); 10.0 stable is the version consistently reported to work.
WINE_VER=${WINE_VER:-10.0.0.0~noble-1}
if ! wine --version 2>/dev/null | grep -q "^wine-${WINE_VER%%.*}\."; then
    echo ">> installing WineHQ stable $WINE_VER"
    sudo dpkg --add-architecture i386
    sudo mkdir -pm755 /etc/apt/keyrings
    sudo wget -qO /etc/apt/keyrings/winehq-archive.key https://dl.winehq.org/wine-builds/winehq.key
    codename=$(lsb_release -cs)
    sudo wget -qNP /etc/apt/sources.list.d/ "https://dl.winehq.org/wine-builds/ubuntu/dists/$codename/winehq-$codename.sources"
    sudo apt-get update -qq
    sudo apt-get install -y -qq --allow-downgrades --install-recommends curl \
        winehq-stable="$WINE_VER" wine-stable="$WINE_VER" wine-stable-amd64="$WINE_VER" wine-stable-i386:i386="$WINE_VER"
    sudo apt-mark hold winehq-stable wine-stable wine-stable-amd64 wine-stable-i386 >/dev/null
    rm -rf "$PREFIX"   # a prefix made by another Wine version is not worth keeping before the terminal is in
fi
if [ ! -f "$TERMINAL" ]; then
    echo ">> creating prefix $PREFIX (win10) and installing MetaTrader 5 silently"
    WINEPREFIX=$PREFIX WINEDEBUG=-all WINEDLLOVERRIDES="mscoree=d;mshtml=d" winecfg -v=win10   # no Mono/Gecko prompts
    curl -fsSL "$SETUP_URL" -o /tmp/mt5setup.exe
    WINEPREFIX=$PREFIX WINEDEBUG=-all wine /tmp/mt5setup.exe /auto || true
    for i in $(seq 1 60); do [ -f "$TERMINAL" ] && break; sleep 5; done
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
MT5_LOGIN=${MT5_LOGIN:-}
MT5_PASSWORD=${MT5_PASSWORD:-}
MT5_SERVER=${MT5_SERVER:-}
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
