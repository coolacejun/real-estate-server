@echo off
setlocal

set "ROOT=%~1"
if "%ROOT%"=="" set "ROOT=C:\land-info-worker"

set "INTERVAL=%~2"
if "%INTERVAL%"=="" set "INTERVAL=10"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_windows_land_info_worker.ps1" -Root "%ROOT%"
if errorlevel 1 exit /b %ERRORLEVEL%

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0register_windows_land_info_worker_task.ps1" -Root "%ROOT%" -IntervalMinutes %INTERVAL%
exit /b %ERRORLEVEL%
