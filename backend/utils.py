"""Shared time utilities (UTC <-> IST)."""

import time
import datetime
from datetime import timezone, timedelta

_IST = timezone(timedelta(hours=5, minutes=30))


def utc_now() -> float:
    return time.time()


def ist_now() -> datetime.datetime:
    return datetime.datetime.now(_IST)


def ist_now_str(fmt: str = "%Y-%m-%d %H:%M:%S IST") -> str:
    return datetime.datetime.now(_IST).strftime(fmt)


def utc_to_ist(ts: float) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(ts, tz=_IST)


def utc_to_ist_str(ts: float, fmt: str = "%Y-%m-%d %H:%M:%S IST") -> str:
    return utc_to_ist(ts).strftime(fmt)


def ist_time_now() -> datetime.time:
    return ist_now().time()


def ist_hm(h: int, m: int) -> datetime.time:
    return datetime.time(h, m, 0)

def _session_minutes(h: int, m: int, expiry_h: int = 13, expiry_m: int = 30) -> int:
    """Maps clock time to minutes since session start."""
    t_min = h * 60 + m
    e_min = expiry_h * 60 + expiry_m
    s_min = e_min + 1
    if t_min >= s_min:
        return t_min - s_min
    else:
        return t_min + (1440 - s_min)

def is_time_before_in_session(h1: int, m1: int, h2: int, m2: int, expiry_h: int, expiry_m: int) -> bool:
    return _session_minutes(h1, m1, expiry_h, expiry_m) < _session_minutes(h2, m2, expiry_h, expiry_m)

def is_time_before_or_equal_in_session(h1: int, m1: int, h2: int, m2: int, expiry_h: int, expiry_m: int) -> bool:
    return _session_minutes(h1, m1, expiry_h, expiry_m) <= _session_minutes(h2, m2, expiry_h, expiry_m)

def is_in_session_range(now_h: int, now_m: int, start_h: int, start_m: int,
                         end_h: int, end_m: int, expiry_h: int, expiry_m: int) -> bool:
    """True if now is within [start, end] using session-relative ordering.
    Handles cross-midnight windows correctly (e.g. 22:10 → 04:00)."""
    now_s   = _session_minutes(now_h,   now_m,   expiry_h, expiry_m)
    start_s = _session_minutes(start_h, start_m, expiry_h, expiry_m)
    end_s   = _session_minutes(end_h,   end_m,   expiry_h, expiry_m)
    return start_s <= now_s <= end_s

def has_reached_session_time(now_h: int, now_m: int, target_h: int, target_m: int,
                              expiry_h: int, expiry_m: int) -> bool:
    """True if now >= target in session-relative ordering.
    Prevents premature force-close when force_close is next-day."""
    now_s    = _session_minutes(now_h,    now_m,    expiry_h, expiry_m)
    target_s = _session_minutes(target_h, target_m, expiry_h, expiry_m)
    return now_s >= target_s

def get_session_boundaries(now_ist: datetime.datetime, expiry_h: int, expiry_m: int):
    expiry_today = now_ist.replace(hour=expiry_h, minute=expiry_m, second=0, microsecond=0)
    if now_ist <= expiry_today:
        session_start = expiry_today - timedelta(days=1) + timedelta(minutes=1)
        session_end = expiry_today
    else:
        session_start = expiry_today + timedelta(minutes=1)
        session_end = expiry_today + timedelta(days=1)
    return session_start, session_end

def session_info_str(expiry_h: int, expiry_m: int) -> dict:
    now_ist = ist_now()
    start, end = get_session_boundaries(now_ist, expiry_h, expiry_m)
    return {
        "session_start": start.strftime("%Y-%m-%d %H:%M"),
        "session_end": end.strftime("%Y-%m-%d %H:%M"),
        "expiry_in_h": expiry_h,
        "expiry_in_m": expiry_m,
        "now_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S")
    }

def is_blackout_day(date_ist: datetime.date, skip_weekends: bool, blackout_dates_csv: str) -> bool:
    if skip_weekends and date_ist.weekday() >= 5:
        return True
    if blackout_dates_csv:
        dates = [d.strip() for d in blackout_dates_csv.split(",") if d.strip()]
        date_str = date_ist.strftime("%Y-%m-%d")
        if date_str in dates:
            return True
    return False
