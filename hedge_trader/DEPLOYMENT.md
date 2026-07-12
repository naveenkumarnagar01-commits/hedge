# Deployment Guide — BTC Straddle Bot

## Requirements

- Docker 24+
- Docker Compose v2
- GNU Make
- Port 8080 open on the server

---

## First-time Setup

```bash
# 1. Clone the repo
git clone https://oauth2:<TOKEN>@gitlab.buopso.care/dhawan-legacy/hedge_platform_btc.git
cd hedge_platform_btc

# 2. Create .env from example
cp .env.example .env

# 3. Deploy with fresh DB
make deploy-fresh
```

Dashboard is live at: **http://\<server-ip\>:8080**

---

## Deploy Commands

| Command | What it does |
|---|---|
| `make deploy-fresh` | New code + **wipe DB** (fresh start) |
| `make deploy-update` | New code + **keep DB** (history preserved) |
| `make rollback VERSION=v1.0` | Revert to a specific version |
| `make status` | Show running containers |
| `make logs` | Live logs (both services) |
| `make logs-bot` | Bot logs only |
| `make logs-dash` | Dashboard logs only |
| `make stop` | Stop containers (DB preserved) |
| `make reset` | Stop + wipe DB completely |

---

## Versioning & Rollback

Every stable release is tagged in git. Tags are the rollback points.

### View available versions
```bash
git fetch --tags
git tag
# v1.0
# v1.1
# v1.2
```

### Deploy latest (fresh DB)
```bash
git pull origin dev
make deploy-fresh      # wipes old DB, starts clean
```

### Deploy latest (keep DB)
```bash
git pull origin dev
make deploy-update     # trade history preserved
```

### Rollback to previous version
```bash
make rollback VERSION=v1.0
# Checks out v1.0, rebuilds image, restarts containers
# DB is preserved (no data loss)
```

### Rollback + fresh DB
```bash
make rollback VERSION=v1.0
make reset             # then wipe DB
make deploy-fresh
```

---

## How Rollback Works

```
Current state:  v1.1 running
                straddle_data volume (DB)

make rollback VERSION=v1.0:
  1. git checkout v1.0          ← old source code
  2. docker compose build       ← build old image
  3. docker compose down        ← stop v1.1 containers
  4. docker compose up -d       ← start v1.0 containers
  5. DB volume untouched        ← trade history safe

Result: v1.0 running with existing DB
```

**DB is always safe during rollback** — only `make reset` or `make deploy-fresh` wipes it.

---

## Architecture

```
┌─────────────────────────────────┐
│         Docker Host             │
│                                 │
│  ┌──────────────────────────┐   │
│  │  straddle-bot:v1.1       │   │
│  │  (straddle_trader.py)    │   │
│  └──────────┬───────────────┘   │
│             │  /data/           │
│  ┌──────────┴───────────────┐   │
│  │  straddle_data (volume)  │   │   ← persists across restarts/rollbacks
│  │  straddle_paper.db       │   │
│  └──────────┬───────────────┘   │
│             │                   │
│  ┌──────────┴───────────────┐   │
│  │  straddle-dashboard:v1.1 │   │
│  │  (dashboard.py :8080)    │   │
│  └──────────────────────────┘   │
└─────────────────────────────────┘
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| Dashboard not loading | `make logs-dash` |
| Bot not scanning | `make logs-bot` |
| Database locked | `docker compose restart bot` |
| Port 8080 in use | Edit `docker-compose.yml` → change `"8080:8080"` to `"8081:8080"` |
| Start completely fresh | `make reset && make deploy-fresh` |
