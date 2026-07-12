-- Hedge Platform — Initial Schema
-- Run: psql $DATABASE_URL -f 001_initial.sql

CREATE TABLE IF NOT EXISTS agent_state (
    key         TEXT PRIMARY KEY,
    value       JSONB NOT NULL,
    updated_at  DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS trade_log (
    id          BIGSERIAL PRIMARY KEY,
    ts_utc      DOUBLE PRECISION NOT NULL,
    ts_ist      TEXT NOT NULL,
    executor    TEXT NOT NULL,
    action      TEXT NOT NULL,
    instrument  TEXT NOT NULL,
    side        TEXT,
    qty         DOUBLE PRECISION,
    price       DOUBLE PRECISION,
    pnl         DOUBLE PRECISION,
    status      TEXT NOT NULL,
    detail      JSONB,
    is_paper    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trade_log_executor   ON trade_log(executor);
CREATE INDEX IF NOT EXISTS idx_trade_log_ts_utc     ON trade_log(ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_trade_log_is_paper   ON trade_log(is_paper);

CREATE TABLE IF NOT EXISTS manager_log (
    id           BIGSERIAL PRIMARY KEY,
    ts_utc       DOUBLE PRECISION NOT NULL,
    ts_ist       TEXT NOT NULL,
    executor     TEXT NOT NULL,
    request_type TEXT NOT NULL,
    status       TEXT NOT NULL,  -- APPROVED | REJECTED
    reason       TEXT,
    detail       JSONB,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_manager_log_ts ON manager_log(ts_utc DESC);

CREATE TABLE IF NOT EXISTS analyst_log (
    id          BIGSERIAL PRIMARY KEY,
    ts_utc      DOUBLE PRECISION NOT NULL,
    ts_ist      TEXT NOT NULL,
    high_line   DOUBLE PRECISION,
    low_line    DOUBLE PRECISION,
    candidates  JSONB,
    touches     JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_config (
    key         TEXT PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_by  TEXT NOT NULL DEFAULT 'system'
);

-- Seed default config values
INSERT INTO system_config (key, value) VALUES
    ('candle_count',             '30'),
    ('candle_tf_minutes',        '5'),
    ('delta_touch',              '30'),
    ('min_touches',              '3'),
    ('dominance_ratio',          '0.75'),
    ('distance_threshold',       '500'),
    ('option_max_premium',       '520'),
    ('option_min_intrinsic',     '300'),
    ('ask_mark_ratio',           '0.10'),
    ('fill_timeout_sec',         '2'),
    ('q_max_btc',                '1.0'),
    ('profit_factor_step1',      '1.10'),
    ('profit_factor_step2_up',   '1.50'),
    ('profit_factor_step2_adv',  '2.00'),
    ('virtual_balance_usdt',     '100000')
ON CONFLICT (key) DO NOTHING;
