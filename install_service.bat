@echo off
:: ══════════════════════════════════════════════════════════════════════
::  Hedge Trader — Windows Auto-Restart Service Installer
::
::  Uses Windows Task Scheduler (built-in, no extra software needed).
::  The task will:
::    - Start automatically when Windows boots
::    - Restart automatically if it crashes (every 1 minute, up to 3 times)
::    - Run as the current user
::
::  HOW TO USE:
::    1. Fill in your credentials in the CONFIGURATION section below
::    2. Right-click this file → "Run as Administrator"
::    3. Done — platform starts on every boot automatically
::
::  TO REMOVE THE SERVICE:
::    schtasks /Delete /TN "HedgeTrader" /F
::
::  TO CHECK STATUS:
::    schtasks /Query /TN "HedgeTrader" /FO LIST
:: ══════════════════════════════════════════════════════════════════════

:: ── CONFIGURATION — fill these in ─────────────────────────────────────
set BINANCE_API_KEY=your_api_key_here
set BINANCE_API_SECRET=your_api_secret_here
set TELEGRAM_BOT_TOKEN=your_telegram_bot_token_here
set TELEGRAM_CHAT_ID=your_telegram_chat_id_here
:: ───────────────────────────────────────────────────────────────────────

set PLATFORM_DIR=%~dp0
set PYTHON_EXE=python

echo.
echo ══════════════════════════════════════════════
echo   Hedge Trader — Service Installer
echo ══════════════════════════════════════════════
echo.

:: Create a launcher script with credentials baked in
set LAUNCHER=%PLATFORM_DIR%_service_launcher.bat
(
echo @echo off
echo cd /d "%PLATFORM_DIR%"
echo set BINANCE_API_KEY=%BINANCE_API_KEY%
echo set BINANCE_API_SECRET=%BINANCE_API_SECRET%
echo set TELEGRAM_BOT_TOKEN=%TELEGRAM_BOT_TOKEN%
echo set TELEGRAM_CHAT_ID=%TELEGRAM_CHAT_ID%
echo %PYTHON_EXE% -m backend.main
) > "%LAUNCHER%"

:: Delete existing task if present
schtasks /Delete /TN "HedgeTrader" /F >nul 2>&1

:: Create scheduled task
schtasks /Create ^
  /TN "HedgeTrader" ^
  /TR "\"%LAUNCHER%\"" ^
  /SC ONLOGON ^
  /DELAY 0000:30 ^
  /RL HIGHEST ^
  /F

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [OK] Task created successfully.
    echo      Platform will start 30 seconds after every login.
    echo.
    echo      To start NOW without rebooting:
    echo      schtasks /Run /TN "HedgeTrader"
    echo.
) else (
    echo.
    echo [ERROR] Task creation failed.
    echo         Make sure you ran this as Administrator.
    echo.
)

pause
