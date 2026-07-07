@echo off
setlocal
chcp 65001 >nul

set BOT_DIR=c:\Users\Admin\OneDrive\Desktop\hedge_platform_backup_20260528_1724
set LOG_DASH=%BOT_DIR%\dash_log.txt
set PORT=8080

REM -- Auto-detect Python: prefer project venv, fall back to system install --
if exist "%BOT_DIR%\.venv\Scripts\python.exe" (
    set PY=%BOT_DIR%\.venv\Scripts\python.exe
) else if exist "C:\Program Files\Python312\python.exe" (
    set PY=C:\Program Files\Python312\python.exe
) else (
    set PY=python
)

echo [%date% %time%] ===== Dashboard starting  port=%PORT% ===== >> "%LOG_DASH%"

REM -- Kill any existing process on port 8080 to avoid conflict --
for /f "tokens=5" %%a in ('netstat -ano 2^>nul ^| findstr ":%PORT% " ^| findstr "LISTENING"') do (
    echo [%date% %time%] Killing old process on port %PORT% PID=%%a >> "%LOG_DASH%"
    taskkill /PID %%a /F >nul 2>&1
)
timeout /t 1 /nobreak >nul

:restart_loop
echo [%date% %time%] Starting dashboard.py >> "%LOG_DASH%"
cd /d "%BOT_DIR%"
"%PY%" dashboard.py --port %PORT% >> "%LOG_DASH%" 2>&1

set EXIT_CODE=%errorlevel%
echo [%date% %time%] Dashboard exited  code=%EXIT_CODE% >> "%LOG_DASH%"

timeout /t 10 /nobreak >nul
goto restart_loop
