@echo off
setlocal
rem Muestra las direcciones sin iniciar Docker: start.bat links
rem Demos Kafka: start.bat inject-alert docker (o podman); opciones 13, 14 y 15.
if "%~1"=="--help" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" -Action help
  exit /b
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" %*
set "result=%errorlevel%"
if "%~1"=="" pause
exit /b %result%
