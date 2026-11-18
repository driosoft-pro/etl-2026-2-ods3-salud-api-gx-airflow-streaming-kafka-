@echo off
setlocal
rem Muestra las direcciones sin iniciar Docker: start.bat links
rem Demos Kafka: start.bat inject-alert 1 (Docker) o 2 (Podman); opciones 13, 14 y 15.
rem Trafico en vivo: opciones 16 a 19; ejemplo: start.bat simulate-valid 1.
if "%~1"=="--help" (
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" -Action help
  exit /b
)
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0scripts\project_manager.ps1" %*
set "result=%errorlevel%"
if "%~1"=="" pause
exit /b %result%
