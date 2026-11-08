-- =====================================================================================
-- Retail Stock schema
-- Sized for ~6M movements/day (70 ops/s average, 1k ops/s bursts).
-- =====================================================================================

-- -------------------------------------------------------------------------------------
-- Current stock position (hot rows, updated under SELECT ... FOR UPDATE).
-- fillfactor < 100 leaves room on each page for HOT updates (no index churn on updates).
-- -------------------------------------------------------------------------------------
CREATE TABLE stock_levels
(
    sku         text        NOT NULL,
    location_id text        NOT NULL,
    on_hand     bigint      NOT NULL CHECK (on_hand >= 0),
    reserved    bigint      NOT NULL CHECK (reserved >= 0),
    version     bigint      NOT NULL CHECK (version >= 0),
    updated_at  timestamptz NOT NULL,
    CONSTRAINT pk_stock_levels PRIMARY KEY (sku, location_id)
) WITH (fillfactor = 80);

CREATE INDEX ix_stock_levels_location ON stock_levels (location_id, sku);

ALTER TABLE stock_levels SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.02);

-- -------------------------------------------------------------------------------------
-- Reservations
-- -------------------------------------------------------------------------------------
CREATE TABLE reservations
(
    id          uuid        NOT NULL,
    sku         text        NOT NULL,
    location_id text        NOT NULL,
    quantity    bigint      NOT NULL CHECK (quantity > 0),
    status      text        NOT NULL CHECK (status IN ('ACTIVE', 'COMMITTED', 'RELEASED', 'EXPIRED')),
    reference   text,
    created_by  text        NOT NULL,
    expires_at  timestamptz NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL,
    CONSTRAINT pk_reservations PRIMARY KEY (id),
    CONSTRAINT fk_reservations_stock FOREIGN KEY (sku, location_id) REFERENCES stock_levels (sku, location_id)
) WITH (fillfactor = 90);

-- Partial indexes stay tiny: only ACTIVE reservations are indexed.
CREATE INDEX ix_reservations_active_expiry ON reservations (expires_at) WHERE status = 'ACTIVE';
CREATE INDEX ix_reservations_active_stock ON reservations (sku, location_id) WHERE status = 'ACTIVE';
CREATE INDEX ix_reservations_closed_at ON reservations (updated_at) WHERE status <> 'ACTIVE';

-- -------------------------------------------------------------------------------------
-- Immutable movement ledger, range-partitioned by month (~180M rows/month).
-- Old partitions can be detached/archived without vacuum storms.
-- -------------------------------------------------------------------------------------
CREATE TABLE stock_movements
(
    id              uuid        NOT NULL,
    sku             text        NOT NULL,
    location_id     text        NOT NULL,
    movement_type   text        NOT NULL,
    on_hand_delta   bigint      NOT NULL,
    reserved_delta  bigint      NOT NULL,
    on_hand_after   bigint      NOT NULL,
    reserved_after  bigint      NOT NULL,
    stock_version   bigint      NOT NULL,
    reason          text,
    reference       text,
    reservation_id  uuid,
    actor           text        NOT NULL,
    idempotency_key text,
    occurred_at     timestamptz NOT NULL,
    CONSTRAINT pk_stock_movements PRIMARY KEY (occurred_at, id)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX ix_stock_movements_stock_time ON stock_movements (sku, location_id, occurred_at DESC);

-- Safety net only: rows land here if partition maintenance ever falls behind.
CREATE TABLE stock_movements_default PARTITION OF stock_movements DEFAULT;

CREATE
OR REPLACE FUNCTION ensure_stock_movement_partitions(p_months_ahead integer DEFAULT 2)
    RETURNS integer
    LANGUAGE plpgsql
AS
$$
DECLARE
v_month_start date := date_trunc('month', (now() AT TIME ZONE 'UTC'))::date;
    v_from
date;
    v_to
date;
    v_name
text;
    v_created
integer := 0;
BEGIN
    -- Serialize concurrent callers (several pods run the maintenance job).
    PERFORM
pg_advisory_xact_lock(hashtext('ensure_stock_movement_partitions'));

FOR i IN 0..GREATEST(p_months_ahead, 0)
        LOOP
            v_from := (v_month_start + make_interval(months => i))::date;
            v_to
:= (v_month_start + make_interval(months => i + 1))::date;
            v_name
:= format('stock_movements_%s', to_char(v_from, 'YYYY_MM'));
            IF
to_regclass(v_name) IS NULL THEN
                EXECUTE format(
                        'CREATE TABLE %I PARTITION OF stock_movements FOR VALUES FROM (%L) TO (%L)',
                        v_name,
                        to_char(v_from, 'YYYY-MM-DD') || ' 00:00:00+00',
                        to_char(v_to, 'YYYY-MM-DD') || ' 00:00:00+00'
                        );
                v_created
:= v_created + 1;
END IF;
END LOOP;
RETURN v_created;
END;
$$;

SELECT ensure_stock_movement_partitions(2);

-- -------------------------------------------------------------------------------------
-- Idempotency keys (scoped per authenticated subject). Purged after the retention window.
-- -------------------------------------------------------------------------------------
CREATE TABLE idempotency_keys
(
    scope           text        NOT NULL,
    idempotency_key text        NOT NULL,
    fingerprint     text        NOT NULL,
    response        text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_idempotency_keys PRIMARY KEY (scope, idempotency_key)
);

CREATE INDEX ix_idempotency_keys_created ON idempotency_keys (created_at);

-- -------------------------------------------------------------------------------------
-- Transactional outbox: written in the same transaction as the stock change,
-- drained to Kafka by the relay. Rows are deleted once acknowledged by the broker,
-- so the table stays small unless the (single) broker is unavailable.
-- -------------------------------------------------------------------------------------
CREATE TABLE event_outbox
(
    id              bigint GENERATED ALWAYS AS IDENTITY,
    event_id        uuid        NOT NULL,
    event_type      text        NOT NULL,
    topic           text        NOT NULL,
    message_key     text        NOT NULL,
    payload         text        NOT NULL,
    attempts        integer     NOT NULL DEFAULT 0,
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    locked_until    timestamptz,
    locked_by       text,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_event_outbox PRIMARY KEY (id),
    CONSTRAINT uq_event_outbox_event UNIQUE (event_id)
);

CREATE INDEX ix_event_outbox_ready ON event_outbox (next_attempt_at, id);

-- High churn table (insert + delete): vacuum aggressively.
ALTER TABLE event_outbox SET (
    autovacuum_vacuum_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_vacuum_cost_delay = 0
    );

-- -------------------------------------------------------------------------------------
-- Dead letter queue (PostgreSQL backed) for both outbound events that exhausted
-- their retries and inbound Kafka messages that could not be processed.
-- -------------------------------------------------------------------------------------
CREATE TABLE dead_letters
(
    id               bigint GENERATED ALWAYS AS IDENTITY,
    source           text        NOT NULL CHECK (source IN ('OUTBOX', 'CONSUMER')),
    event_id         uuid,
    event_type       text,
    topic            text        NOT NULL,
    message_key      text,
    payload          text        NOT NULL,
    source_partition integer,
    source_offset    bigint,
    attempts         integer     NOT NULL DEFAULT 0,
    error            text,
    status           text        NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING', 'REPLAYED', 'DISCARDED')),
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT pk_dead_letters PRIMARY KEY (id)
);

CREATE INDEX ix_dead_letters_status_created ON dead_letters (status, created_at);
