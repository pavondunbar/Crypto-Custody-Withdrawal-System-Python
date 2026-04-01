-- -----------------------------------------------
-- Extended grants for readonly_user.
-- The reconciliation service and event consumer
-- connect as readonly_user with selective write
-- access to audit/reconciliation/DLQ tables.
-- -----------------------------------------------

-- Read access to core ledger tables and views
GRANT SELECT ON journal_entries TO readonly_user;
GRANT SELECT ON accounts TO readonly_user;
GRANT SELECT ON chart_of_accounts TO readonly_user;
GRANT SELECT ON account_balances TO readonly_user;
GRANT SELECT ON transactions TO readonly_user;
GRANT SELECT ON transaction_status_history TO readonly_user;
GRANT SELECT ON transaction_current_status TO readonly_user;

-- Audit events: insert-only
GRANT INSERT ON audit_events TO readonly_user;

-- Reconciliation: insert runs, update status, insert mismatches
GRANT SELECT ON reconciliation_runs TO readonly_user;
GRANT INSERT ON reconciliation_runs TO readonly_user;
GRANT UPDATE ON reconciliation_runs TO readonly_user;
GRANT SELECT ON reconciliation_mismatches TO readonly_user;
GRANT INSERT ON reconciliation_mismatches TO readonly_user;

-- Dead letter queue: insert and controlled update
GRANT SELECT ON dead_letter_queue TO readonly_user;
GRANT INSERT ON dead_letter_queue TO readonly_user;
GRANT UPDATE ON dead_letter_queue TO readonly_user;

-- Roles table: read-only
GRANT SELECT ON roles TO readonly_user;
