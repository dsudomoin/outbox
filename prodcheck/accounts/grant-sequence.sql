-- Run by accounts_owner after schema.sql. The start check demands SELECT on the sequence, which USAGE does not give,
-- and neither roles.sql nor the migration grants it: without this file the app refuses to start.
grant select on sequence accounts.account_events_id_seq to accounts_rw;
