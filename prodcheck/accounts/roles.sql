-- Run by the superuser: what the infrastructure provides. The service's migrations grant nothing.
create role accounts_rw login password 'accounts_rw';
create role accounts_owner login password 'accounts_owner';
create schema accounts authorization accounts_owner;
grant usage on schema accounts to accounts_rw;
alter default privileges for role accounts_owner in schema accounts
  grant select, insert, update, delete on tables to accounts_rw;
alter default privileges for role accounts_owner in schema accounts
  grant usage on sequences to accounts_rw;
