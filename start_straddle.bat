@echo off
setlocal
chcp 65001 >nul

set BOT_DIR=c:\Users\Admin\OneDrive\Desktop\hedge_platform_backup_20260528_1724
set LOG_BOT=%BOT_DIR%\run_log.txt

REM -- Auto-detect Python: prefer project venv, fall back to system install --
if exist "%BOT_DIR%\.venv\Scripts\python.exe" (
    set PY=%BOT_DIR%\.venv\Scripts\python.exe
) else if exist "C:\Program Files\Python312\python.exe" (
    set PY=C:\Program Files\Python312\python.exe
) else (
    set PY=python
)

echo [%date% %time%] ===== StraddleBot starting  PY=%PY% ===== >> "%LOG_BOT%"

:restart_loop

REM -- Clean stale DB lock files left by a crash --
if exist "%BOT_DIR%\straddle_paper.db-wal" (
    del /f /q "%BOT_DIR%\straddle_paper.db-wal"
    echo [%date% %time%] Cleaned stale .db-wal >> "%LOG_BOT%"
)
if exist "%BOT_DIR%\straddle_paper.db-shm" (
    del /f /q "%BOT_DIR%\straddle_paper.db-shm"
    echo [%date% %time%] Cleaned stale .db-shm >> "%LOG_BOT%"
)

echo [%date% %time%] Starting straddle_trader.py >> "%LOG_BOT%"
cd /d "%BOT_DIR%"
"%PY%" straddle_trader.py >> "%LOG_BOT%" 2>&1

set EXIT_CODE=%errorlevel%
echo [%date% %time%] Process exited  code=%EXIT_CODE% >> "%LOG_BOT%"

if %EXIT_CODE% EQU 0 (
    echo [%date% %time%] Clean exit — restarting in 10s >> "%LOG_BOT%"
    timeout /t 10 /nobreak >nul
) else (
    echo [%date% %time%] Crash exit — restarting in 15s >> "%LOG_BOT%"
    timeout /t 15 /nobreak >nul
)

goto restart_loop
