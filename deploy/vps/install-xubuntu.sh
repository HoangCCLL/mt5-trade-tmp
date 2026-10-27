#!/usr/bin/env bash
# One-shot setup on a clean Xubuntu 24.04 VPS: WineHQ devel + MT5 terminal + the agent exe + three systemd user units.
# Run as the trade user, over SSH or in the desktop; a desktop session (xrdp or console) must be logged in.
#   GATEWAY_URL=wss://host ACCOUNT_ID=demo2 AGENT_TOKEN=... MT5_LOGIN=123 MT5_PASSWORD=... MT5_SERVER=Broker-Demo bash install-xubuntu.sh
# Verified 2026-10-27: Wine 11.20 + Windows 11 profile + terminal build 6286. Wine 10.0/11.0 or a win10 profile make
# terminal64.exe abort with "A debugger has been found running in your system" (https://www.mql5.com/en/forum/503128).
set -euo pipefail
: "${GATEWAY_URL:?}" "${ACCOUNT_ID:?}" "${AGENT_TOKEN:?}"
EXE_URL=${EXE_URL:-https://github.com/HoangCCLL/mt5-trade-tmp/releases/download/latest/mt5agent.exe}
SETUP_URL=${SETUP_URL:-https://download.metatrader.com/cdn/web/metaquotes.software.corp/mt5/mt5setup.exe}
# ucrtbase.dll: Wine's builtin lacks crealf (numpy needs it); winetricks' ucrtbase2019 source, extracted without cabextract.
UCRT_URL=${UCRT_URL:-https://download.visualstudio.microsoft.com/download/pr/85d47aa9-69ae-4162-8300-e6b7e4bf3cf3/52B196BBE9016488C735E7B41805B651261FFA5D7AA86EB6A1D0095BE83687B2/VC_redist.x64.exe}
PREFIX=${WINEPREFIX:-$HOME/.mt5}
TERMINAL="$PREFIX/drive_c/Program Files/MetaTrader 5/terminal64.exe"
AGENT="$PREFIX/drive_c/mt5agent"

if [ -z "${DISPLAY:-}" ]; then  # over SSH: borrow the display of the running desktop session (xrdp = :10, console = :0)
    sp=$(pgrep -u "$USER" -x xfce4-session | head -1)
    DISPLAY=$(tr '\0' '\n' < "/proc/${sp:-0}/environ" 2>/dev/null | sed -n 's/^DISPLAY=//p')
    [ -n "$DISPLAY" ] || { echo "no desktop session found; log in via RDP/console first or pass DISPLAY="; exit 1; }
fi
export DISPLAY XAUTHORITY=${XAUTHORITY:-$HOME/.Xauthority} WINEPREFIX=$PREFIX WINEDEBUG=-all
echo ">> using DISPLAY=$DISPLAY"

if ! wine --version 2>/dev/null | grep -qE "^wine-11\.([1-9]|[1-9][0-9])"; then
    echo ">> installing WineHQ devel (11.1+)"
    sudo dpkg --add-architecture i386
    sudo mkdir -pm755 /etc/apt/keyrings
    sudo wget -qO /etc/apt/keyrings/winehq-archive.key https://dl.winehq.org/wine-builds/winehq.key
    codename=$(lsb_release -cs)
    sudo wget -qNP /etc/apt/sources.list.d/ "https://dl.winehq.org/wine-builds/ubuntu/dists/$codename/winehq-$codename.sources"
    sudo apt-get update -qq
    sudo apt-get install -y -qq --install-recommends winehq-devel curl python3-pip
fi

if [ ! -f "$TERMINAL" ]; then
    echo ">> creating prefix $PREFIX (Windows 11 profile) and installing MetaTrader 5 silently"
    WINEDLLOVERRIDES="mscoree=d;mshtml=d" winecfg -v=win11
    curl -fsSL "$SETUP_URL" -o /tmp/mt5setup.exe
    wine /tmp/mt5setup.exe /auto || true
    for i in $(seq 1 60); do [ -f "$TERMINAL" ] && break; sleep 5; done
    [ -f "$TERMINAL" ] || { echo "terminal64.exe not found at $TERMINAL"; exit 1; }
    wineserver -k || true
fi
WINEDLLOVERRIDES="mscoree=d;mshtml=d" winecfg -v=win11

if [ ! -f /tmp/ucrtbase.dll ]; then
    echo ">> extracting native ucrtbase.dll"
    curl -fsSL "$UCRT_URL" -o /tmp/vc_redist2019.x64.exe
    python3 -m pip install -q --user cabarchive 2>/dev/null || pip3 install -q --user --break-system-packages cabarchive
    python3 - <<'PY'
import cabarchive
data = open("/tmp/vc_redist2019.x64.exe", "rb").read()
off = 0
while (i := data.find(b"MSCF", off)) >= 0:
    off = i + 4
    try: arc = cabarchive.CabArchive(data[i:])
    except Exception: continue
    for name, f in arc.items():
        if f.buf[:4] == b"MSCF":
            try: inner = cabarchive.CabArchive(f.buf)
            except Exception: continue
            if "ucrtbase.dll" in inner:
                open("/tmp/ucrtbase.dll", "wb").write(inner["ucrtbase.dll"].buf); raise SystemExit
raise SystemExit("ucrtbase.dll not found in redistributable")
PY
fi
cp /tmp/ucrtbase.dll "$PREFIX/drive_c/windows/system32/ucrtbase.dll"

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

# The SDK also needs Tools > Options > Expert Advisors > "Allow algorithmic trading via external Python API".
# The terminal keeps that flag outside common.ini/terminal.ini, so it is the one manual click (orders fail with
# retcode 10027 "AutoTrading disabled by client" until it is ticked; no restart needed afterwards).

# Desktop session must never blank or lock: the terminal needs a live X display.
xfconf-query -c xfce4-screensaver -p /saver/enabled -n -t bool -s false 2>/dev/null || true
xfconf-query -c xfce4-screensaver -p /lock/enabled -n -t bool -s false 2>/dev/null || true
xfconf-query -c xfce4-power-manager -p /xfce4-power-manager/dpms-enabled -n -t bool -s false 2>/dev/null || true

mkdir -p ~/.config/systemd/user
cat > ~/.config/systemd/user/mt5-terminal.service <<UNIT
[Unit]
Description=MetaTrader 5 terminal (Wine)
[Service]
Environment=WINEPREFIX=$PREFIX WINEDEBUG=-all DISPLAY=$DISPLAY XAUTHORITY=$XAUTHORITY WINEDLLOVERRIDES=ucrtbase=b
ExecStart=/usr/bin/wine "C:\\\\Program Files\\\\MetaTrader 5\\\\terminal64.exe"
Restart=always
RestartSec=10
[Install]
WantedBy=default.target
UNIT
for role in trader streamer; do
cat > ~/.config/systemd/user/mt5-$role.service <<UNIT
[Unit]
Description=MT5 agent $role
After=mt5-terminal.service
[Service]
Environment=WINEPREFIX=$PREFIX WINEDEBUG=-all DISPLAY=$DISPLAY XAUTHORITY=$XAUTHORITY WINEDLLOVERRIDES=ucrtbase=n,b
ExecStart=/usr/bin/wine C:\\\\mt5agent\\\\mt5agent.exe $role
Restart=always
RestartSec=5
[Install]
WantedBy=default.target
UNIT
done
systemctl --user daemon-reload
sudo loginctl enable-linger "$USER"

echo ">> starting terminal, then smoke test (exe under Wine + SDK sees the terminal + clock offsets)"
systemctl --user enable --now mt5-terminal; sleep 45
WINEDLLOVERRIDES="ucrtbase=n,b" wine "C:\\mt5agent\\mt5agent.exe" timecheck
systemctl --user enable --now mt5-trader mt5-streamer
systemctl --user --no-pager status mt5-terminal mt5-trader mt5-streamer | grep -E "mt5-|Active"
echo ">> logs: journalctl --user -fu mt5-trader   |  $AGENT/logs/"
echo ">> LAST MANUAL STEP (once, in the desktop): Tools > Options > Expert Advisors > tick \"Allow algorithmic trading via external Python API\" > OK"
