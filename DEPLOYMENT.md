# Hedge Trader Platform — Deployment Guide

**Version:** 1.0 (as of 2026-05-28)  
**Stack:** FastAPI · PostgreSQL 15 · Redis 7 · Nginx · Docker Compose

---

## 1. Architecture Overview

```
Browser
  │
  ├── HTTP GET /          → panel.html (served by FastAPI)
  ├── REST  /api/*        → FastAPI (port 8000)
  └── WS    /ws           → FastAPI WebSocket feed
          ↑ (proxied by Nginx on port 3000 in containerised mode)

FastAPI Backend (port 8000)
  ├── AnalystAgent        — computes H/L lines daily at configurable IST time
  ├── ManagerAgent        — risk gate: approves / rejects position requests
  ├── BullishExecutor     — long futures + ITM PUT hedge  ($100k virtual balance)
  ├── BearishExecutor     — short futures + ITM CALL hedge ($100k virtual balance)
  ├── VolatileTrader      — macro-event straddle           ($100k virtual balance)
  ├── Data feeds          — Spot WS, Futures WS, Options WS (Binance public)
  └── EventCalendar       — macro event scheduler for VolatileTrader

PostgreSQL 15          — agent_state, trade_log, manager_log, analyst_log, system_config
Redis 7                — optional pub/sub message bus between processes
```

**Session window:** Binance options expire at 13:30 IST daily.  
Each trading session runs 13:31 IST → 13:30 IST next day.

---

## 2. Prerequisites

| Requirement | Version |
|---|---|
| Docker | 24+ |
| Docker Compose | v2 (`docker compose` not `docker-compose`) |
| Binance account | Futures + Options trade permissions |
| OS | Linux (recommended), macOS, or Windows with WSL 2 |

Outbound ports needed: **443** (Binance REST/WS), **5432** (Postgres), **6379** (Redis).

---

## 3. Repository Layout

```
hedge_platform/
├── backend/
│   ├── main.py                  # entry point — starts all agents
│   ├── config.py                # runtime config dataclass
│   ├── state_store.py           # async Postgres KV + crash recovery
│   ├── message_bus.py           # in-process pub/sub (+ optional Redis)
│   ├── utils.py                 # IST time helpers, session math
│   ├── telegram_alert.py        # startup/shutdown notifications
│   ├── api/
│   │   └── gateway.py           # FastAPI app, REST + WebSocket endpoints
│   ├── agents/
│   │   ├── analyst.py
│   │   ├── manager.py
│   │   ├── bullish_executor.py
│   │   ├── bearish_executor.py
│   │   └── volatile_trader.py
│   ├── data/
│   │   ├── spot_feed.py
│   │   ├── futures_feed.py
│   │   ├── options_feed.py
│   │   ├── event_calendar.py
│   │   └── ws_manager.py
│   ├── execution/
│   │   └── paper_engine.py      # $100k virtual paper account per trader
│   └── db/
│       └── migrations/
│           └── 001_initial.sql  # schema + seed data — auto-applied on first boot
├── frontend/
│   ├── Dockerfile.frontend      # Vite build → Nginx
│   └── nginx.conf               # proxies /api/* and /ws to backend:8000
├── Dockerfile.backend
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## 4. Environment Configuration

### 4.1 Create `.env`

```bash
cp .env.example .env
```

Edit `.env` — **do not commit this file**:

```dotenv
# Binance — Futures + Options trade permissions required for live orders
BINANCE_API_KEY=<your_api_key>
BINANCE_API_SECRET=<your_api_secret>

# Database (matches docker-compose defaults — change for production)
DATABASE_URL=postgresql://hedge:hedge123@postgres:5432/hedgedb

# Redis (optional — omit or leave empty to run without Redis)
REDIS_URL=redis://redis:6379

# Server
HOST=0.0.0.0
PORT=8000

# Logging: DEBUG | INFO | WARNING | ERROR
LOG_LEVEL=INFO
```

### 4.2 Production hardening — change defaults

| Variable | docker-compose default | Production recommendation |
|---|---|---|
| `POSTGRES_PASSWORD` | `hedge123` | Use a strong random password; set identically in `DATABASE_URL` |
| `POSTGRES_USER` | `hedge` | Fine to keep or rename |
| Redis | no auth | Add `requirepass` via `redis.conf` volume |

---

## 5. Database Setup

The migration file `backend/db/migrations/001_initial.sql` is mounted into the Postgres container at `/docker-entrypoint-initdb.d/` and **runs automatically on first boot** when the data volume is empty.

Tables created:

| Table | Purpose |
|---|---|
| `agent_state` | Key/value crash-recovery state for all agents |
| `trade_log` | Every paper (and future live) trade execution |
| `manager_log` | Risk gate APPROVED / REJECTED decisions |
| `analyst_log` | Historical H/L line computations |
| `system_config` | Persisted user-facing configuration (updated via `/api/config`) |

To re-run the migration manually (e.g., against an existing DB):

```bash
psql "$DATABASE_URL" -f backend/db/migrations/001_initial.sql
```

---

## 6. Docker Compose Deployment

### 6.1 Build and start all services

```bash
docker compose up --build -d
```

Start-up order enforced by `depends_on` health checks:

```
postgres (healthy) ─┐
                     ├→ backend ─→ frontend
redis    (healthy) ─┘
```

The backend waits for Postgres (`pg_isready`) and Redis (`redis-cli ping`) before starting.

### 6.2 Verify health

```bash
docker compose ps
# All services should show "running (healthy)" or "running"

curl http://localhost:8000/api/status
# Returns: spot/futures feed state, options count, WS client count

curl http://localhost:8000/api/session-info
# Returns: current session window + expiry countdown
```

### 6.3 Open the dashboard

Navigate to **http://localhost:3000** (frontend via Nginx)  
or **http://localhost:8000** (backend directly — FastAPI serves `panel.html` at `GET /`).

### 6.4 Stop

```bash
docker compose down          # keeps postgres_data volume
docker compose down -v       # also deletes the database volume (destructive)
```

---

## 7. Service Ports

| Service | Container port | Host port | Purpose |
|---|---|---|---|
| `postgres` | 5432 | 5432 | PostgreSQL |
| `redis` | 6379 | 6379 | Redis pub/sub |
| `backend` | 8000 | 8000 | FastAPI REST + WebSocket |
| `frontend` | 3000 | 3000 | Nginx (reverse proxy + static) |

For production, expose only port **3000** externally and block 5432 / 6379 / 8000 at the firewall.

---

## 8. API Reference

All endpoints are served from `http://host:8000`.

### System

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/status` | Feed status, WS client count |
| `GET` | `/api/session-info` | Current session window + countdown |
| `GET` | `/api/orderbook` | Spot and futures order book snapshot |

### Analyst

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/analyst` | Current H/L lines + recent candles |
| `POST` | `/api/analyst/run` | Trigger manual analyst run |

### Trading

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/positions` | All executor statuses |
| `GET` | `/api/forward` | Paper trading PnL summary |
| `POST` | `/api/forward/reset` | Reset all paper accounts |
| `GET` | `/api/candles` | Last N candles (`?n=300&tf=5m`) |
| `GET` | `/api/options-chain` | Current options chain snapshot |

### Volatile Trader

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/volatile/force-close` | Force-close all open straddle legs |
| `POST` | `/api/volatile/clear-memory` | Wipe volatile state without orders |

### Journal

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/journal/sessions` | Session-wise PnL with filters |
| `GET` | `/api/journal/summary` | Aggregate PnL summary |

### Configuration

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/config` | Current runtime config |
| `POST` | `/api/config` | Update config params (422 on validation error) |

### WebSocket

| Path | Description |
|---|---|
| `ws://host:8000/ws` | Real-time event stream (all bus events). Send `"ping"` → receives `{"type":"pong"}`. Last 100 events replayed on connect. |

---

## 9. Startup Sequence (Backend)

On `fastapi_startup` the backend executes these steps in order:

1. Connect to PostgreSQL (`state_store.connect`)
2. Restore persisted user config from `agent_state` table
3. Connect to Redis (optional; skipped if `REDIS_URL` is empty)
4. Pre-fetch historical candles: 500×5m, 500×15m, 300×1h, 200×4h
5. Start live WebSocket feeds: spot, futures, options
6. Start EventCalendar
7. Start agents: Analyst → Manager → Bullish → Bearish → Volatile
8. Schedule daily squareoff broadcasters (one per trader)
9. Start 5-second state persister (crash recovery loop)
10. Send Telegram startup notification

---

## 10. Logging and Monitoring

### Logs

```bash
# All services
docker compose logs -f

# Backend only
docker compose logs -f backend

# Follow with timestamps
docker compose logs -f --timestamps backend
```

Log format:  
`YYYY-MM-DD HH:MM:SS [agent-name         ] LEVEL: message`

### Telegram Alerts

The platform sends a Telegram message on startup and shutdown.  
To enable, add to `.env`:

```dotenv
TELEGRAM_BOT_TOKEN=<your_bot_token>
TELEGRAM_CHAT_ID=<your_chat_id>
```

If not configured, `telegram_alert.py` silently no-ops.

### State persistence

Agent state is written to Postgres every **5 seconds** via `_state_persister`.  
On restart, config and executor state are automatically restored — no manual intervention needed.

---

## 11. Development / Local Run (without Docker)

```bash
# Create and activate virtual environment
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux / macOS

# Install dependencies
pip install -r requirements.txt

# Set environment variables
cp .env.example .env
# Edit .env with your Binance keys and local Postgres URL

# Start backend
python -m backend.main
# Dashboard available at http://localhost:8000
```

Requires Postgres and Redis running locally (or update `.env` to point at Docker containers).

---

## 12. Production Deployment Checklist

- [ ] Strong Postgres password in `.env` and `docker-compose.yml`
- [ ] Redis authentication configured (`requirepass`)
- [ ] `.env` excluded from version control (`.gitignore`)
- [ ] Only port 3000 exposed externally (firewall rules)
- [ ] TLS termination via reverse proxy (e.g., Nginx/Caddy in front of port 3000)
- [ ] `LOG_LEVEL=INFO` (not DEBUG) in production
- [ ] Telegram alerts configured for startup/shutdown visibility
- [ ] Postgres data volume backed up regularly (`postgres_data`)
- [ ] `CORS allow_origins=["*"]` narrowed to your actual domain in `gateway.py`
- [ ] Monitor `/api/status` for feed disconnects (spot/futures/options)

---

## 13. Upgrading

```bash
# Pull latest code
git pull

# Rebuild and restart
docker compose up --build -d

# Check logs for migration errors
docker compose logs backend | head -50
```

New SQL migrations (if any) should be placed in `backend/db/migrations/` and applied manually:

```bash
docker compose exec postgres psql -U hedge -d hedgedb -f /path/to/migration.sql
```

---

## 14. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Backend exits immediately | Postgres not ready | Check `docker compose ps`; backend health check retries 10× |
| `price = 0` in all executors | Double-import of `backend.main` | Ensure `python -m backend.main` (not `python backend/main.py`) |
| Feed shows disconnected | Binance WS rate limit or network | Check backend logs; feeds auto-reconnect |
| `422 Unprocessable Entity` on `/api/config` | Invalid config value | Response body contains list of validation errors |
| Panel shows no data after refresh | WS not connected | Check browser console; WS replays last 100 events on reconnect |
| `UnicodeEncodeError` on Windows console | Console not UTF-8 | Platform auto-reconfigures stdout; run inside Docker to avoid |
