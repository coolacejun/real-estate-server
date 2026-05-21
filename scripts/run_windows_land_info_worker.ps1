param(
  [string]$Root = "C:\land-info-worker",
  [switch]$DryRun,
  [switch]$KeepBrowserOpen
)

$ErrorActionPreference = "Stop"
$Python = Join-Path $Root "worker\.venv\Scripts\LandInfoWorkerRunner.exe"
if (-not (Test-Path $Python)) {
  $Python = Join-Path $Root "worker\.venv\Scripts\python.exe"
}
$Script = Join-Path $Root "worker\windows_land_info_worker.py"
$LogDir = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

if (-not (Test-Path $Python)) {
  throw "Python venv not found. Run install_windows_land_info_worker.ps1 first."
}
if (-not (Test-Path $Script)) {
  throw "Worker script not found: $Script"
}

$Stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$LogFile = Join-Path $LogDir "land_info_worker_$Stamp.log"
$Args = @($Script, "--root", $Root)
if ($DryRun) { $Args += "--dry-run" }
if ($KeepBrowserOpen) { $Args += "--keep-browser-open" }

& $Python @Args 2>&1 | Tee-Object -FilePath $LogFile
