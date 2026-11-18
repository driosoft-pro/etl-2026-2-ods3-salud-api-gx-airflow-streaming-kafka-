@echo off
setlocal
rem Muestra las direcciones sin iniciar Docker: start.bat links
if "%~1"=="--help" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" -Action help
  exit /b
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" %*
set "result=%errorlevel%"
if "%~1"=="" pause
exit /b %result%
