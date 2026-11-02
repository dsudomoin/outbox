create table if not exists pc_key_seq (topic text not null, key text not null, seq bigint not null, primary key (topic, key));
-- one row per committed message, inserted in the same transaction as the send
create table if not exists pc_sent (message_id uuid primary key, app text not null, instance text not null,
  outbox text not null, topic text not null, key text, seq bigint not null, tag text not null,
  contract int not null default 1, at timestamptz not null default clock_timestamp());
-- messages whose transaction was rolled back on purpose: id written in a separate transaction after the rollback
create table if not exists pc_rolled_back (message_id uuid primary key, topic text not null, key text);
-- every arrival at a sink, committed on its own BEFORE any effect (duplicates show here)
create table if not exists pc_arrival (no bigserial primary key, sink text not null, message_id uuid not null,
  topic text, key text, seq bigint, tag text, instance text, contract int, at timestamptz not null default clock_timestamp());
-- the effect, committed atomically with the dedupe; the primary key makes a double effect an error
create table if not exists pc_effect (no bigserial primary key, sink text not null, message_id uuid not null,
  topic text, key text, seq bigint, tag text, contract int, at timestamptz not null default clock_timestamp(),
  unique (sink, message_id));
-- own dedupe of the DB route (route `ledger`, see below)
create table if not exists pc_dedupe (message_id uuid primary key);
-- a target that counts: the inbox accepted a message whose effect exists
create table if not exists pc_double_effect (no bigserial primary key, sink text not null, message_id uuid not null, at timestamptz not null default clock_timestamp());
