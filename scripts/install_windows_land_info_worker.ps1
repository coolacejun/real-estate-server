param(
  [string]$Root = "C:\land-info-worker",
  [switch]$Run
)

$ErrorActionPreference = "Stop"

$WorkerDir = Join-Path $Root "worker"
$RequestsDir = Join-Path $Root "requests"
$DownloadsDir = Join-Path $Root "downloads"
$ManifestsDir = Join-Path $Root "manifests"
$LogsDir = Join-Path $Root "logs"
$VenvDir = Join-Path $WorkerDir ".venv"

function Resolve-SystemPython {
  $candidates = @(
    @{ Exe = "py"; Args = @("-3") },
    @{ Exe = "python"; Args = @() },
    @{ Exe = "python3"; Args = @() }
  )

  foreach ($candidate in $candidates) {
    $command = Get-Command $candidate.Exe -ErrorAction SilentlyContinue
    if (-not $command) {
      continue
    }

    try {
      & $command.Source @($candidate.Args + @("--version")) *> $null
      if ($LASTEXITCODE -eq 0) {
        return @{
          Exe = $command.Source
          Args = $candidate.Args
        }
      }
    } catch {
      continue
    }
  }

  $pathCandidates = @()
  $pathCandidates += Get-ChildItem "$env:LOCALAPPDATA\Programs\Python\Python*\python.exe" -ErrorAction SilentlyContinue
  $pathCandidates += Get-ChildItem "$env:ProgramFiles\Python*\python.exe" -ErrorAction SilentlyContinue
  if (${env:ProgramFiles(x86)}) {
    $pathCandidates += Get-ChildItem "${env:ProgramFiles(x86)}\Python*\python.exe" -ErrorAction SilentlyContinue
  }

  foreach ($candidatePath in $pathCandidates | Sort-Object FullName -Descending) {
    try {
      & $candidatePath.FullName --version *> $null
      if ($LASTEXITCODE -eq 0) {
        return @{
          Exe = $candidatePath.FullName
          Args = @()
        }
      }
    } catch {
      continue
    }
  }

  throw "Python 3 was not found. Install Python 3.11+ from https://www.python.org/downloads/windows/ and enable 'Add python.exe to PATH', then run this setup again."
}

New-Item -ItemType Directory -Force -Path $WorkerDir, $RequestsDir, $DownloadsDir, $ManifestsDir, $LogsDir | Out-Null

if (-not (Test-Path (Join-Path $WorkerDir "worker_config.json"))) {
  Copy-Item (Join-Path $WorkerDir "worker_config.example.json") (Join-Path $WorkerDir "worker_config.json")
  Write-Host "Created worker_config.json. Fill vworld_user_id/vworld_user_password before running." -ForegroundColor Yellow
}

if (-not (Test-Path $VenvDir)) {
  $SystemPython = Resolve-SystemPython
  Write-Host "Using Python: $($SystemPython.Exe) $($SystemPython.Args -join ' ')"
  & $SystemPython.Exe @($SystemPython.Args + @("-m", "venv", $VenvDir))
}

$Python = Join-Path $VenvDir "Scripts\python.exe"
$PythonW = Join-Path $VenvDir "Scripts\pythonw.exe"
$WorkerRunner = Join-Path $VenvDir "Scripts\LandInfoWorkerRunner.exe"
$WorkerStatus = Join-Path $VenvDir "Scripts\LandInfoWorkerStatus.exe"

if ((Test-Path $Python) -and (-not (Test-Path $WorkerRunner))) {
  Copy-Item $Python $WorkerRunner
}
if (Test-Path $PythonW) {
  if (-not (Test-Path $WorkerStatus)) {
    Copy-Item $PythonW $WorkerStatus
  }
} elseif ((Test-Path $Python) -and (-not (Test-Path $WorkerStatus))) {
  Copy-Item $Python $WorkerStatus
}

& $Python -m pip install --upgrade pip
& $Python -m pip install playwright pywinauto psutil

if ($Run) {
  $RunPython = $WorkerRunner
  if (-not (Test-Path $RunPython)) {
    $RunPython = $Python
  }
  & $RunPython (Join-Path $WorkerDir "windows_land_info_worker.py") --root $Root
}
