# ── Straddle Bot — Deployment Makefile ───────────────────────────────────────
#
# Usage:
#   make deploy-fresh          Deploy latest code, WIPE DB (fresh start)
#   make deploy-update         Deploy latest code, KEEP existing DB
#   make rollback VERSION=v1.0 Rollback to a specific version, keep DB
#   make status                Show running containers
#   make logs                  Follow live logs (both services)
#   make logs-bot              Bot logs only
#   make logs-dash             Dashboard logs only
#   make stop                  Stop all containers (keep DB)
#   make reset                 Stop all + wipe DB completely

VERSION ?= $(shell git describe --tags --abbrev=0 2>/dev/null || echo "latest")

.PHONY: deploy-fresh deploy-update rollback build tag status logs logs-bot logs-dash stop reset

# ── Build image tagged with current git version ───────────────────────────────
build:
	@echo "==> Building image  version=$(VERSION)"
	APP_VERSION=$(VERSION) docker compose build --no-cache
	@echo "==> Build complete: straddle-bot:$(VERSION)"

# ── Tag current images as backup before overwriting ──────────────────────────
tag:
	@echo "==> Saving current images as backup..."
	-docker tag straddle-bot:$(VERSION) straddle-bot:rollback-backup 2>/dev/null || true
	-docker tag straddle-dashboard:$(VERSION) straddle-dashboard:rollback-backup 2>/dev/null || true

# ── Deploy: fresh code + WIPE DB (new session, zero history) ─────────────────
deploy-fresh: tag build
	@echo "==> Stopping containers and wiping DB..."
	APP_VERSION=$(VERSION) docker compose down -v
	@echo "==> Starting fresh..."
	APP_VERSION=$(VERSION) docker compose up -d
	@echo ""
	@echo "✓ Deployed $(VERSION) with FRESH DB"
	@echo "  Dashboard: http://$(shell hostname -I | awk '{print $$1}'):8080"

# ── Deploy: fresh code + KEEP DB (trade history preserved) ───────────────────
deploy-update: tag build
	@echo "==> Restarting with updated code (DB preserved)..."
	APP_VERSION=$(VERSION) docker compose down
	APP_VERSION=$(VERSION) docker compose up -d
	@echo ""
	@echo "✓ Deployed $(VERSION) — DB preserved"
	@echo "  Dashboard: http://$(shell hostname -I | awk '{print $$1}'):8080"

# ── Rollback to a previous version ───────────────────────────────────────────
# Usage: make rollback VERSION=v1.0
rollback:
	@echo "==> Rolling back to $(VERSION)..."
	git fetch --tags
	git checkout $(VERSION)
	APP_VERSION=$(VERSION) docker compose build --no-cache
	APP_VERSION=$(VERSION) docker compose down
	APP_VERSION=$(VERSION) docker compose up -d
	@echo ""
	@echo "✓ Rolled back to $(VERSION) — DB preserved"
	@echo "  To return to latest: git checkout dev"

# ── Status / Logs ─────────────────────────────────────────────────────────────
status:
	docker compose ps

logs:
	docker compose logs -f

logs-bot:
	docker compose logs -f bot

logs-dash:
	docker compose logs -f dashboard

# ── Stop / Reset ──────────────────────────────────────────────────────────────
stop:
	docker compose down
	@echo "✓ Stopped (DB preserved)"

reset:
	@echo "==> WARNING: This will DELETE all trade history!"
	@read -p "Type YES to confirm: " c; [ "$$c" = "YES" ] && docker compose down -v && echo "✓ Reset complete" || echo "Cancelled"
