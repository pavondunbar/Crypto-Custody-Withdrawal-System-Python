-- ===============================================
-- Missing Components: Audit, RBAC, Reconciliation,
-- State Machine, Dead Letter Queue
-- ===============================================

-- -----------------------------------------------
-- TABLE: audit_events (append-only)
-- -----------------------------------------------

CREATE TABLE audit_events(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  request_id UUID,
  trace_id UUID,
  actor_role VARCHAR(32) NOT NULL,
  actor_id VARCHAR(128) NOT NULL,
  action VARCHAR(64) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id UUID,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_trace_id
  ON audit_events(trace_id);

CREATE INDEX idx_audit_resource
  ON audit_events(resource_type, resource_id);

CREATE OR REPLACE FUNCTION trg_deny_update_audit()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'audit_events is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_audit
  BEFORE UPDATE ON audit_events
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_audit();

CREATE OR REPLACE FUNCTION trg_deny_delete_audit()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'audit_events is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_audit
  BEFORE DELETE ON audit_events
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_audit();

-- -----------------------------------------------
-- TABLE: roles (RBAC)
-- -----------------------------------------------

CREATE TABLE roles(
  code VARCHAR(32) PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  permissions JSONB NOT NULL DEFAULT '[]'
);

INSERT INTO roles (code, name, permissions) VALUES
  ('admin', 'Administrator',
   '["approve_withdrawal", "view_transactions"]'),
  ('system', 'System Service',
   '["publish_events", "reconcile"]'),
  ('signer', 'Transaction Signer',
   '["sign_transaction"]');

-- -----------------------------------------------
-- TABLE: reconciliation_runs
-- -----------------------------------------------

CREATE TABLE reconciliation_runs(
  run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  status VARCHAR(16) NOT NULL DEFAULT 'running'
    CHECK (status IN ('running', 'passed', 'failed', 'error')),
  total_accounts INT NOT NULL DEFAULT 0,
  mismatches_found INT NOT NULL DEFAULT 0
);

CREATE OR REPLACE FUNCTION trg_reconciliation_runs_update()
RETURNS TRIGGER AS $$
BEGIN
  IF OLD.status <> 'running' THEN
    RAISE EXCEPTION
      'reconciliation_runs: cannot update a finalized run (status=%).  '
      'Only running -> final transitions allowed.', OLD.status;
  END IF;
  IF NEW.status = 'running' THEN
    RAISE EXCEPTION
      'reconciliation_runs: new status must be a terminal state, '
      'not running';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER guard_reconciliation_runs_update
  BEFORE UPDATE ON reconciliation_runs
  FOR EACH ROW EXECUTE FUNCTION trg_reconciliation_runs_update();

CREATE OR REPLACE FUNCTION trg_deny_delete_reconciliation_runs()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'reconciliation_runs is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_reconciliation_runs
  BEFORE DELETE ON reconciliation_runs
  FOR EACH ROW
  EXECUTE FUNCTION trg_deny_delete_reconciliation_runs();

-- -----------------------------------------------
-- TABLE: reconciliation_mismatches (append-only)
-- -----------------------------------------------

CREATE TABLE reconciliation_mismatches(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES reconciliation_runs(run_id),
  account_id UUID NOT NULL REFERENCES accounts(id),
  asset VARCHAR(10) NOT NULL,
  expected_balance DECIMAL(38,18) NOT NULL,
  actual_balance DECIMAL(38,18) NOT NULL,
  difference DECIMAL(38,18) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION trg_deny_update_recon_mismatches()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'reconciliation_mismatches is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_recon_mismatches
  BEFORE UPDATE ON reconciliation_mismatches
  FOR EACH ROW
  EXECUTE FUNCTION trg_deny_update_recon_mismatches();

CREATE OR REPLACE FUNCTION trg_deny_delete_recon_mismatches()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'reconciliation_mismatches is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_recon_mismatches
  BEFORE DELETE ON reconciliation_mismatches
  FOR EACH ROW
  EXECUTE FUNCTION trg_deny_delete_recon_mismatches();

-- -----------------------------------------------
-- TABLE: dead_letter_queue
-- -----------------------------------------------

CREATE TABLE dead_letter_queue(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  original_event_id UUID NOT NULL REFERENCES outbox_events(id),
  error_message TEXT NOT NULL,
  retry_count INT NOT NULL DEFAULT 0,
  max_retries INT NOT NULL DEFAULT 5,
  last_retry_at TIMESTAMPTZ,
  resolved_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_dlq_unresolved
  ON dead_letter_queue(created_at)
  WHERE resolved_at IS NULL;

CREATE OR REPLACE FUNCTION trg_deny_delete_dlq()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'dead_letter_queue: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_dlq
  BEFORE DELETE ON dead_letter_queue
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_dlq();

CREATE OR REPLACE FUNCTION trg_dlq_update_guard()
RETURNS TRIGGER AS $$
BEGIN
  IF NEW.original_event_id <> OLD.original_event_id
     OR NEW.error_message <> OLD.error_message
     OR NEW.max_retries <> OLD.max_retries
     OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION
      'dead_letter_queue: only retry_count, last_retry_at, '
      'and resolved_at may be updated';
  END IF;
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER guard_dlq_update
  BEFORE UPDATE ON dead_letter_queue
  FOR EACH ROW EXECUTE FUNCTION trg_dlq_update_guard();

-- -----------------------------------------------
-- TRIGGER: state machine on transaction_status_history
-- -----------------------------------------------

CREATE OR REPLACE FUNCTION trg_enforce_status_transition()
RETURNS TRIGGER AS $$
DECLARE
  current_status VARCHAR(20);
BEGIN
  SELECT status INTO current_status
    FROM transaction_status_history
    WHERE transaction_id = NEW.transaction_id
    ORDER BY created_at DESC
    LIMIT 1;

  -- First status must be pending_policy
  IF current_status IS NULL THEN
    IF NEW.status <> 'pending_policy' THEN
      RAISE EXCEPTION
        'First status must be pending_policy, got %',
        NEW.status;
    END IF;
    RETURN NEW;
  END IF;

  -- Terminal states block further transitions
  IF current_status IN ('rejected', 'confirmed', 'failed') THEN
    RAISE EXCEPTION
      'Cannot transition from terminal state %',
      current_status;
  END IF;

  -- Valid transitions
  IF current_status = 'pending_policy'
     AND NEW.status IN ('approved', 'rejected') THEN
    RETURN NEW;
  END IF;

  IF current_status = 'approved'
     AND NEW.status IN ('signed', 'failed') THEN
    RETURN NEW;
  END IF;

  IF current_status = 'signed'
     AND NEW.status IN ('broadcast', 'failed') THEN
    RETURN NEW;
  END IF;

  IF current_status = 'broadcast'
     AND NEW.status IN ('confirmed', 'failed') THEN
    RETURN NEW;
  END IF;

  RAISE EXCEPTION
    'Invalid status transition: % -> %',
    current_status, NEW.status;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER enforce_status_transition
  BEFORE INSERT ON transaction_status_history
  FOR EACH ROW
  EXECUTE FUNCTION trg_enforce_status_transition();
