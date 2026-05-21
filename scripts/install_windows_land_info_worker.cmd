@echo off
setlocal

set "ROOT=%~1"
if "%ROOT%"=="" set "ROOT=C:\land-info-worker"

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0install_windows_land_info_worker.ps1" -Root "%ROOT%"
exit /b %ERRORLEVEL%
