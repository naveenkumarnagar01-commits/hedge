@echo off
:: ══════════════════════════════════════════════════════
::  Hedge Trader — Manual Run Script
::  Double-click this to start the platform manually.
::  For auto-restart, use install_service.bat instead.
:: ══════════════════════════════════════════════════════

cd /d "%~dp0"

:: ── Set your credentials here ──────────────────────────
set BINANCE_API_KEY=your_api_key_here
set BINANCE_API_SECRET=your_api_secret_here

:: ── Optional: Telegram alerts ──────────────────────────
:: set TELEGRAM_BOT_TOKEN=your_bot_token_here
:: set TELEGRAM_CHAT_ID=your_chat_id_here

:: ── Start server ───────────────────────────────────────
echo Starting Hedge Trader at %TIME%...
python -m backend.main

pause
