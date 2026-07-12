"""
Telegram alert system for Hedge Trader platform.

Set these environment variables:
  TELEGRAM_BOT_TOKEN  — from @BotFather
  TELEGRAM_CHAT_ID    — your personal or group chat ID

If either is missing, all calls are silently ignored (no crash).
"""

import json
import logging
import os
import threading
import urllib.request

log = logging.getLogger("telegram")

_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
_CHAT_ID   = os.environ.get("TELEGRAM_CHAT_ID", "")


def send(message: str):
    """Fire-and-forget Telegram message. Never blocks the caller."""
    if not _BOT_TOKEN or not _CHAT_ID:
        return
    threading.Thread(target=_send_sync, args=(message,), daemon=True).start()


def _send_sync(message: str):
    try:
        url  = f"https://api.telegram.org/bot{_BOT_TOKEN}/sendMessage"
        body = json.dumps({
            "chat_id":    _CHAT_ID,
            "text":       message,
            "parse_mode": "HTML",
        }).encode()
        req = urllib.request.Request(
            url, data=body,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")
