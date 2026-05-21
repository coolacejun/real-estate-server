param(
  [string]$Root = "C:\land-info-worker",
  [int]$IntervalMinutes = 10,
  [string]$TaskName = "LandInfoWorker"
)

$ErrorActionPreference = "Stop"
$RunScript = Join-Path $Root "worker\run_windows_land_info_worker.ps1"
if (-not (Test-Path $RunScript)) {
  throw "Run script not found: $RunScript"
}

$Command = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File `"$RunScript`" -Root `"$Root`""
$User = "$env:USERDOMAIN\$env:USERNAME"

Write-Host "Registering scheduled task $TaskName for $User every $IntervalMinutes minutes."
Write-Host "UI automation requires this task to run only when the user is logged on." -ForegroundColor Yellow

schtasks.exe /Create /TN $TaskName /SC MINUTE /MO $IntervalMinutes /TR $Command /RU $User /IT /F

Write-Host "Registered. You can run it now with:"
Write-Host "schtasks /Run /TN $TaskName"
