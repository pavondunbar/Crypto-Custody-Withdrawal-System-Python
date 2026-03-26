-- -----------------------------------------------
-- Read-only user for the outbox publisher.
-- The publisher only needs SELECT on outbox_events
-- and UPDATE on the published_at column.
-- -----------------------------------------------

CREATE USER readonly_user WITH PASSWORD 'readonly_pass';

GRANT CONNECT ON DATABASE ledger_db TO readonly_user;
GRANT USAGE ON SCHEMA public TO readonly_user;

GRANT SELECT ON outbox_events TO readonly_user;
GRANT UPDATE (published_at) ON outbox_events TO readonly_user;
