# Supervisor loop for one agent role. Started by the scheduled task at logon; restarts python if it exits.
param([Parameter(Mandatory)][ValidateSet("trader", "streamer")][string]$Role)
$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$Log = Join-Path $Root "logs\$Role.log"
New-Item -ItemType Directory -Force (Join-Path $Root "logs") | Out-Null
Set-Location $Root

Get-Content (Join-Path $Root "agent.env") | ForEach-Object {
    if ($_ -match '^\s*([^#][^=]*)=(.*)$') { [Environment]::SetEnvironmentVariable($Matches[1].Trim(), $Matches[2].Trim(), "Process") }
}

while ($true) {
    if ((Test-Path $Log) -and (Get-Item $Log).Length -gt 20MB) { Move-Item -Force $Log "$Log.1" }
    "$(Get-Date -Format s) [supervisor] starting $Role" | Add-Content $Log
    & py -3.11 -m "agent.$Role" *>> $Log
    "$(Get-Date -Format s) [supervisor] $Role exited ($LASTEXITCODE), restarting in 5s" | Add-Content $Log
    Start-Sleep -Seconds 5
}
