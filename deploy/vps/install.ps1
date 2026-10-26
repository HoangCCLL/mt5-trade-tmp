<#
  Installs the MT5 agent (trader + streamer) on a Windows VPS as two scheduled tasks that start at logon
  of the current user and run in the interactive session (the MT5 terminal needs a desktop).
  Usage (PowerShell as the trading user, from the repo checkout):
      .\deploy\vps\install.ps1             # install / update
      .\deploy\vps\install.ps1 -Uninstall
  Prereq: copy deploy\vps\agent.env.example to deploy\vps\agent.env and fill it in.
#>
param([switch]$Uninstall, [string]$Root = "C:\mt5agent")
$ErrorActionPreference = "Stop"
$Roles = "trader", "streamer"
$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")

if ($Uninstall) {
    foreach ($r in $Roles) { Unregister-ScheduledTask -TaskName "mt5-$r" -Confirm:$false -ErrorAction SilentlyContinue }
    Get-Process python* -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match "agent\.(trader|streamer)" } | Stop-Process -Force
    Write-Host "removed tasks; files left in $Root"
    exit 0
}

if (-not (Test-Path (Join-Path $PSScriptRoot "agent.env"))) { throw "deploy\vps\agent.env missing (copy agent.env.example)" }

# 1. Python 3.11 (winget is present on Server 2022 / Win10+; otherwise install manually from python.org)
if (-not (py -3.11 -c "print(1)" 2>$null)) {
    winget install -e --id Python.Python.3.11 --silent --accept-package-agreements --accept-source-agreements
}
py -3.11 -m pip install --quiet --upgrade MetaTrader5 websockets

# 2. Files
New-Item -ItemType Directory -Force $Root, (Join-Path $Root "logs") | Out-Null
foreach ($r in $Roles) { Unregister-ScheduledTask -TaskName "mt5-$r" -Confirm:$false -ErrorAction SilentlyContinue }
Get-Process python* -ErrorAction SilentlyContinue | Where-Object { $_.CommandLine -match "agent\.(trader|streamer)" } | Stop-Process -Force
Copy-Item -Recurse -Force (Join-Path $RepoRoot "agent") $Root
Copy-Item -Force (Join-Path $PSScriptRoot "run-agent.ps1"), (Join-Path $PSScriptRoot "agent.env") $Root

# 3. Scheduled tasks: at logon, interactive session, restart forever
$user = "$env:USERDOMAIN\$env:USERNAME"
foreach ($r in $Roles) {
    $action = New-ScheduledTaskAction -Execute "powershell.exe" `
        -Argument "-NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File `"$Root\run-agent.ps1`" -Role $r" -WorkingDirectory $Root
    $trigger = New-ScheduledTaskTrigger -AtLogOn -User $user
    $settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) `
        -ExecutionTimeLimit ([TimeSpan]::Zero) -MultipleInstances IgnoreNew -StartWhenAvailable
    Register-ScheduledTask -TaskName "mt5-$r" -Action $action -Trigger $trigger -Settings $settings -User $user -RunLevel Highest | Out-Null
    Start-ScheduledTask -TaskName "mt5-$r"
}

# 4. Clock: TTL rejects commands if the VPS drifts
w32tm /resync | Out-Null
Write-Host "installed. logs: $Root\logs\{trader,streamer}.log"
Get-ScheduledTask -TaskName "mt5-*" | Format-Table TaskName, State
