-- ===============================================
-- Double-Entry Accounting Schema
-- Crypto Custody Withdrawal System
-- ===============================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------
-- TABLES
-- -----------------------------------------------

CREATE TABLE accounts(
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  asset VARCHAR(10) NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE chart_of_accounts(
  code VARCHAR(32) PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  account_type VARCHAR(16) NOT NULL
    CHECK (account_type IN ('asset', 'liability', 'revenue')),
  normal_balance VARCHAR(8) NOT NULL
    CHECK (normal_balance IN ('debit', 'credit'))
);

CREATE TABLE journal_entries(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  journal_id UUID NOT NULL,
  account_id UUID NOT NULL REFERENCES accounts(id),
  coa_code VARCHAR(32) NOT NULL
    REFERENCES chart_of_accounts(code),
  asset VARCHAR(10) NOT NULL,
  debit DECIMAL(38,18) NOT NULL DEFAULT 0,
  credit DECIMAL(38,18) NOT NULL DEFAULT 0,
  entry_type VARCHAR(32) NOT NULL,
  reference_id UUID NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_debit_xor_credit CHECK (
    (debit > 0 AND credit = 0)
    OR (debit = 0 AND credit > 0)
  )
);

CREATE TABLE transactions(
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES accounts(id),
  type VARCHAR(20) NOT NULL,
  amount DECIMAL(38,18) NOT NULL,
  destination_address VARCHAR(256),
  idempotency_key VARCHAR(256) UNIQUE,
  policy_check_result JSONB,
  created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE transaction_status_history(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id UUID NOT NULL REFERENCES transactions(id),
  status VARCHAR(20) NOT NULL,
  tx_hash VARCHAR(256),
  block_number BIGINT,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE outbox_events(
  id UUID PRIMARY KEY,
  aggregate_id VARCHAR(256) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at TIMESTAMPTZ
);

-- -----------------------------------------------
-- INDEXES
-- -----------------------------------------------

CREATE INDEX idx_journal_account_coa
  ON journal_entries(account_id, coa_code);

CREATE INDEX idx_journal_id
  ON journal_entries(journal_id);

CREATE INDEX idx_status_history_tx_created
  ON transaction_status_history(transaction_id, created_at DESC);

CREATE INDEX idx_transactions_account
  ON transactions(account_id);

CREATE INDEX idx_outbox_unpublished
  ON outbox_events(created_at)
  WHERE published_at IS NULL;

-- -----------------------------------------------
-- VIEWS
-- -----------------------------------------------

CREATE VIEW account_balances AS
  SELECT
    account_id,
    asset,
    SUM(credit) - SUM(debit) AS balance
  FROM journal_entries
  WHERE coa_code = 'CUST_LIABILITY'
  GROUP BY account_id, asset;

CREATE VIEW transaction_current_status AS
  SELECT DISTINCT ON (transaction_id)
    transaction_id,
    status,
    tx_hash,
    block_number,
    metadata,
    created_at
  FROM transaction_status_history
  ORDER BY transaction_id, created_at DESC;

-- -----------------------------------------------
-- TRIGGERS: append-only enforcement
-- -----------------------------------------------

CREATE OR REPLACE FUNCTION trg_deny_update_journal()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_journal
  BEFORE UPDATE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_journal();

CREATE OR REPLACE FUNCTION trg_deny_delete_journal()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_journal
  BEFORE DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_journal();

CREATE OR REPLACE FUNCTION trg_deny_update_transactions()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transactions is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_transactions
  BEFORE UPDATE ON transactions
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_transactions();

CREATE OR REPLACE FUNCTION trg_deny_delete_transactions()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transactions is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_transactions
  BEFORE DELETE ON transactions
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_transactions();

CREATE OR REPLACE FUNCTION trg_deny_update_status_history()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'transaction_status_history is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_status_history
  BEFORE UPDATE ON transaction_status_history
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_status_history();

CREATE OR REPLACE FUNCTION trg_deny_delete_status_history()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION
    'transaction_status_history is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_status_history
  BEFORE DELETE ON transaction_status_history
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_status_history();

-- -----------------------------------------------
-- TRIGGER: journal balance constraint (deferred)
-- -----------------------------------------------

CREATE OR REPLACE FUNCTION trg_check_journal_balance()
RETURNS TRIGGER AS $$
DECLARE
  total_debit  DECIMAL(38,18);
  total_credit DECIMAL(38,18);
BEGIN
  SELECT SUM(debit), SUM(credit)
    INTO total_debit, total_credit
    FROM journal_entries
    WHERE journal_id = NEW.journal_id;

  IF total_debit <> total_credit THEN
    RAISE EXCEPTION
      'Unbalanced journal %: debit=% credit=%',
      NEW.journal_id, total_debit, total_credit;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER check_journal_balance
  AFTER INSERT ON journal_entries
  DEFERRABLE INITIALLY DEFERRED
  FOR EACH ROW EXECUTE FUNCTION trg_check_journal_balance();

-- -----------------------------------------------
-- SEED: Chart of Accounts
-- -----------------------------------------------

INSERT INTO chart_of_accounts (code, name, account_type, normal_balance)
VALUES
  ('CUST_LIABILITY',
   'Customer Liability', 'liability', 'credit'),
  ('HOT_WALLET',
   'Hot Wallet', 'asset', 'debit'),
  ('WITHDRAWAL_PENDING',
   'Withdrawal Pending', 'liability', 'credit'),
  ('FEE_REVENUE',
   'Fee Revenue', 'revenue', 'credit');
