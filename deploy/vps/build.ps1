# Build dist\mt5agent.exe. Must run on Windows (PyInstaller does not cross-compile).
# Run with: powershell -ExecutionPolicy Bypass -File deploy\vps\build.ps1
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..\..")
py -3.11 -m pip install --quiet --upgrade pyinstaller MetaTrader5 websockets
py -3.11 -m PyInstaller --onefile --noconfirm --clean --name mt5agent --paths . `
    --collect-all MetaTrader5 --hidden-import numpy --exclude-module agent.mt5_fake agent\__main__.py
Write-Host "built dist\mt5agent.exe"
