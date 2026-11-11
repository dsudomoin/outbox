-- Run by accounts_owner. The first part is the migration as the service has it, statement for statement.

CREATE TABLE accounts.account_events (
    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    shard smallint NOT NULL,
    xid xid8 NOT NULL DEFAULT pg_current_xact_id(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    message_id uuid NOT NULL,
    topic text NOT NULL,
    key bytea,
    payload bytea,
    headers bytea
) PARTITION BY RANGE (xid);

CREATE INDEX account_events_shard_xid
    ON accounts.account_events (shard, xid);


CREATE TABLE accounts.account_events_tick (
    id bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    snapshot pg_snapshot NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    max_id bigint,
    dirty bit varying,
    shards smallint[],
    PRIMARY KEY (id)
);

CREATE INDEX account_events_tick_xmax
    ON accounts.account_events_tick
    ((pg_snapshot_xmax(snapshot)));


CREATE TABLE accounts.account_events_cursor (
    shard smallint NOT NULL,
    seq bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    tick_id bigint NOT NULL,
    target_tick_id bigint,
    last_id bigint NOT NULL DEFAULT 0,
    paused_until timestamptz,
    failures integer NOT NULL DEFAULT 0,
    retry_due_at timestamptz,
    parked_due_at timestamptz,
    PRIMARY KEY (shard, seq)
);


CREATE TABLE accounts.account_events_checkpoint (
    seq bigint GENERATED ALWAYS AS IDENTITY (CACHE 1),
    tick_id bigint NOT NULL,
    busy bit varying NOT NULL,
    busy_tick_id bigint NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (seq)
);


CREATE TABLE accounts.account_events_retry (
    id bigint NOT NULL,
    shard smallint NOT NULL,
    message_id uuid NOT NULL,
    topic text NOT NULL,
    key bytea,
    payload bytea,
    headers bytea,
    created_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    next_attempt_at timestamptz,
    last_error text,
    requeues integer NOT NULL DEFAULT 0,
    waiting_since timestamptz,
    PRIMARY KEY (id)
);

CREATE INDEX account_events_retry_key
    ON accounts.account_events_retry
    (shard, topic, key, id);

CREATE INDEX account_events_retry_due
    ON accounts.account_events_retry
    (shard, next_attempt_at)
    WHERE next_attempt_at IS NOT NULL;

CREATE INDEX account_events_retry_waiting
    ON accounts.account_events_retry
    (waiting_since)
    WHERE waiting_since IS NOT NULL;


CREATE TABLE accounts.account_events_parked (
    id bigint NOT NULL,
    shard smallint NOT NULL,
    message_id uuid NOT NULL,
    topic text NOT NULL,
    key bytea,
    payload bytea,
    headers bytea,
    created_at timestamptz NOT NULL,
    attempts integer NOT NULL DEFAULT 0,
    requeues integer NOT NULL DEFAULT 0,
    due_at timestamptz NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX account_events_parked_key
    ON accounts.account_events_parked
    (shard, topic, key, id);

CREATE INDEX account_events_parked_due
    ON accounts.account_events_parked
    (shard, due_at);


CREATE TABLE accounts.account_events_dead (
    id bigint NOT NULL,
    shard smallint NOT NULL,
    message_id uuid NOT NULL,
    topic text NOT NULL,
    key bytea,
    payload bytea,
    headers bytea,
    created_at timestamptz NOT NULL,
    attempts integer NOT NULL,
    failed_at timestamptz NOT NULL DEFAULT now(),
    error text,
    status text NOT NULL DEFAULT 'kept',
    due_at timestamptz,
    handler_attempts integer NOT NULL DEFAULT 0,
    last_handler_error text,
    redrives integer NOT NULL DEFAULT 0,
    PRIMARY KEY (id)
);

ALTER TABLE accounts.account_events_dead
    ADD CONSTRAINT account_events_dead_status
    CHECK (status IN ('pending', 'kept', 'scheduled'))
    NOT VALID;

CREATE INDEX account_events_dead_due
    ON accounts.account_events_dead
    (due_at)
    WHERE due_at IS NOT NULL;


CREATE TABLE accounts.account_events_redriven (
    message_id uuid NOT NULL,
    redrives integer NOT NULL,
    at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (message_id)
);


CREATE TABLE accounts.account_events_halt (
    id integer NOT NULL DEFAULT 1,
    halted_at timestamptz NOT NULL DEFAULT now(),
    max_tick_xmax xid8 NOT NULL,
    recovery_failed text,
    PRIMARY KEY (id),
    CHECK (id = 1)
);


CREATE TABLE accounts.account_events_maintenance (
    id integer NOT NULL DEFAULT 1,
    succeeded_at timestamptz NOT NULL,
    tail_oversized_since timestamptz,
    PRIMARY KEY (id),
    CHECK (id = 1)
);


CREATE TABLE accounts.account_events_stranded (
    shard smallint NOT NULL,
    stranded_tick_id bigint NOT NULL,
    cursor_seq bigint NOT NULL,
    tick_id bigint NOT NULL,
    PRIMARY KEY (shard)
);


CREATE TABLE accounts.account_events_dropping (
    table_name text NOT NULL,
    at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (table_name)
);


CREATE TABLE accounts.account_events_instance (
    instance_id text NOT NULL,
    shards integer NOT NULL,
    last_seen timestamptz NOT NULL DEFAULT now(),
    hook_active boolean NOT NULL DEFAULT false,
    expires_at timestamptz,
    relay boolean NOT NULL DEFAULT false,
    contracts jsonb,
    routes text[],
    PRIMARY KEY (instance_id)
);

-- Business tables and the checks of this app, not part of that migration.

CREATE TABLE accounts.subscription (
    account text,
    id uuid,
    end_ts timestamptz
);

CREATE TABLE accounts.state (
    account text PRIMARY KEY,
    version bigint NOT NULL
);

CREATE TABLE accounts.blocker (
    account text PRIMARY KEY
);

CREATE TABLE accounts.seen (
    message_id uuid PRIMARY KEY,
    id bigint NOT NULL,
    created_at timestamptz NOT NULL,
    topic text NOT NULL,
    key text,
    payload_md5 text NOT NULL,
    headers_text text NOT NULL,
    max_requeues integer NOT NULL,
    max_attempts integer NOT NULL
);

CREATE TABLE accounts.mismatch (
    no bigserial PRIMARY KEY,
    message_id uuid NOT NULL,
    reason text NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE accounts.handle_called (
    no bigserial PRIMARY KEY,
    message_id uuid NOT NULL,
    at timestamptz NOT NULL DEFAULT clock_timestamp()
);
