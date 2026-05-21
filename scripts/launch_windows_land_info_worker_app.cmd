@echo off
setlocal

set "ROOT=%~1"
if "%ROOT%"=="" set "ROOT=C:\land-info-worker"

set "STATUS=%ROOT%\worker\.venv\Scripts\LandInfoWorkerStatus.exe"
set "RUNNER=%ROOT%\worker\.venv\Scripts\LandInfoWorkerRunner.exe"
set "PYTHONW=%ROOT%\worker\.venv\Scripts\pythonw.exe"
set "PYTHON=%ROOT%\worker\.venv\Scripts\python.exe"
set "APP=%ROOT%\worker\windows_land_info_worker_app.py"

if exist "%STATUS%" (
  start "" "%STATUS%" "%APP%" "%ROOT%"
  exit /b 0
)

if exist "%PYTHONW%" (
  start "" "%PYTHONW%" "%APP%" "%ROOT%"
  exit /b 0
)

if exist "%PYTHON%" (
  start "" "%PYTHON%" "%APP%" "%ROOT%"
  exit /b 0
)

echo Python venv not found. Run setup_windows_land_info_worker.cmd first.
exit /b 1
