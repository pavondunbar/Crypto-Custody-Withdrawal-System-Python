-- ===============================================
-- Double-Entry Accounting Schema
-- Crypto Custody Withdrawal System
-- ===============================================

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------
-- TABLES
-- -----------------------------------------------

-- Identity-only account record (no mutable balance columns).
-- Used as a lock target via SELECT ... FOR UPDATE.
CREATE TABLE accounts(
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  asset VARCHAR(10) NOT NULL,
  created_at TIMESTAMP NOT NULL
);

-- Chart of accounts: reference data for ledger categories.
CREATE TABLE chart_of_accounts(
  code VARCHAR(32) PRIMARY KEY,
  name VARCHAR(128) NOT NULL,
  account_type VARCHAR(16) NOT NULL
    CHECK (account_type IN ('asset', 'liability', 'revenue')),
  normal_balance VARCHAR(8) NOT NULL
    CHECK (normal_balance IN ('debit', 'credit'))
);

-- Core append-only double-entry ledger.
-- Each row is one leg of a journal entry.
-- A journal_id groups a balanced debit/credit pair.
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
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  CONSTRAINT chk_debit_xor_credit CHECK (
    (debit > 0 AND credit = 0)
    OR (debit = 0 AND credit > 0)
  )
);

-- Immutable transaction record.
-- No status, tx_hash, or block_number — those live in
-- transaction_status_history.
CREATE TABLE transactions(
  id UUID PRIMARY KEY,
  account_id UUID NOT NULL REFERENCES accounts(id),
  type VARCHAR(20) NOT NULL,
  amount DECIMAL(38,18) NOT NULL,
  destination_address VARCHAR(256),
  idempotency_key VARCHAR(256) UNIQUE,
  policy_check_result JSONB,
  created_at TIMESTAMP NOT NULL
);

-- Append-only status history. Latest row per transaction_id
-- is the current status.
CREATE TABLE transaction_status_history(
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  transaction_id UUID NOT NULL REFERENCES transactions(id),
  status VARCHAR(20) NOT NULL,
  tx_hash VARCHAR(256),
  block_number BIGINT,
  metadata JSONB,
  created_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Operational outbox (unchanged — UPDATE on published_at is allowed).
CREATE TABLE outbox_events(
  id UUID PRIMARY KEY,
  aggregate_id VARCHAR(256) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  published_at TIMESTAMP
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

-- Derived balance per account.
-- For liability accounts (CUST_LIABILITY), balance = SUM(credit) - SUM(debit).
CREATE VIEW account_balances AS
  SELECT
    account_id,
    asset,
    SUM(credit) - SUM(debit) AS balance
  FROM journal_entries
  WHERE coa_code = 'CUST_LIABILITY'
  GROUP BY account_id, asset;

-- Current status per transaction (latest row wins).
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

-- journal_entries: no UPDATE
CREATE OR REPLACE FUNCTION trg_deny_update_journal()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_journal
  BEFORE UPDATE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_journal();

-- journal_entries: no DELETE
CREATE OR REPLACE FUNCTION trg_deny_delete_journal()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'journal_entries is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_journal
  BEFORE DELETE ON journal_entries
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_journal();

-- transactions: no UPDATE
CREATE OR REPLACE FUNCTION trg_deny_update_transactions()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transactions is append-only: UPDATE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_update_transactions
  BEFORE UPDATE ON transactions
  FOR EACH ROW EXECUTE FUNCTION trg_deny_update_transactions();

-- transactions: no DELETE
CREATE OR REPLACE FUNCTION trg_deny_delete_transactions()
RETURNS TRIGGER AS $$
BEGIN
  RAISE EXCEPTION 'transactions is append-only: DELETE denied';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER deny_delete_transactions
  BEFORE DELETE ON transactions
  FOR EACH ROW EXECUTE FUNCTION trg_deny_delete_transactions();

-- transaction_status_history: no UPDATE
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

-- transaction_status_history: no DELETE
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
-- TRIGGER: journal balance constraint
-- Deferred constraint trigger — runs at COMMIT.
-- Ensures SUM(debit) = SUM(credit) per journal_id.
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

-- ===============================================
-- DEMO WALKTHROUGH
-- ===============================================

-- -----------------------------------------------
-- STEP 1: Create account
-- -----------------------------------------------

INSERT INTO accounts (id, user_id, asset, created_at)
VALUES (
  gen_random_uuid(),
  gen_random_uuid(),
  'ETH',
  NOW()
);

SELECT 'Account created' AS step;
SELECT * FROM accounts;

-- -----------------------------------------------
-- STEP 2: Deposit 100 ETH (fund the account)
-- DEBIT HOT_WALLET, CREDIT CUST_LIABILITY
-- -----------------------------------------------

BEGIN;

WITH acct AS (SELECT id FROM accounts WHERE asset = 'ETH' LIMIT 1),
     jid AS (SELECT gen_random_uuid() AS val)
INSERT INTO journal_entries
  (journal_id, account_id, coa_code, asset, debit, credit,
   entry_type, reference_id)
SELECT jid.val, acct.id, 'HOT_WALLET', 'ETH',
       100.000000000000000000, 0,
       'deposit', acct.id
FROM acct, jid
UNION ALL
SELECT jid.val, acct.id, 'CUST_LIABILITY', 'ETH',
       0, 100.000000000000000000,
       'deposit', acct.id
FROM acct, jid;

COMMIT;

SELECT 'Deposit recorded — balance should be 100' AS step;
SELECT * FROM account_balances;

-- -----------------------------------------------
-- STEP 3: Initiate withdrawal of 25 ETH
-- DEBIT CUST_LIABILITY, CREDIT WITHDRAWAL_PENDING
-- -----------------------------------------------

BEGIN;

WITH acct AS (
  SELECT id FROM accounts WHERE asset = 'ETH' LIMIT 1
  FOR UPDATE
),
balance_check AS (
  SELECT balance FROM account_balances
  WHERE account_id = (SELECT id FROM acct)
),
tx AS (
  INSERT INTO transactions
    (id, account_id, type, amount,
     destination_address, idempotency_key, created_at)
  SELECT
    gen_random_uuid(), acct.id, 'withdrawal',
    25.000000000000000000,
    'bc1qxy2kgdygjrsqtzq2n0yrf', gen_random_uuid()::VARCHAR,
    NOW()
  FROM acct
  RETURNING id, account_id, amount
),
status_row AS (
  INSERT INTO transaction_status_history
    (transaction_id, status, created_at)
  SELECT tx.id, 'pending_policy', NOW()
  FROM tx
  RETURNING transaction_id
),
jid AS (SELECT gen_random_uuid() AS val),
journal AS (
  INSERT INTO journal_entries
    (journal_id, account_id, coa_code, asset, debit, credit,
     entry_type, reference_id)
  SELECT jid.val, tx.account_id, 'CUST_LIABILITY', 'ETH',
         tx.amount, 0, 'withdrawal_initiation', tx.id
  FROM tx, jid
  UNION ALL
  SELECT jid.val, tx.account_id, 'WITHDRAWAL_PENDING', 'ETH',
         0, tx.amount, 'withdrawal_initiation', tx.id
  FROM tx, jid
  RETURNING id
),
outbox AS (
  INSERT INTO outbox_events
    (id, aggregate_id, event_type, payload, created_at)
  SELECT
    gen_random_uuid(), tx.id::VARCHAR,
    'withdrawal.pending_policy',
    jsonb_build_object(
      'transaction_id', tx.id::VARCHAR,
      'amount', tx.amount::VARCHAR,
      'destination', 'bc1qxy2kgdygjrsqtzq2n0yrf'
    ),
    NOW()
  FROM tx
  RETURNING id
)
SELECT 'Withdrawal initiated' AS result;

COMMIT;

SELECT 'After initiation — balance should be 75' AS step;
SELECT * FROM account_balances;
SELECT * FROM transaction_current_status;
SELECT * FROM outbox_events ORDER BY created_at;

-- -----------------------------------------------
-- STEP 4: Confirm withdrawal on-chain
-- DEBIT WITHDRAWAL_PENDING, CREDIT HOT_WALLET
-- -----------------------------------------------

BEGIN;

WITH tx AS (
  SELECT t.id, t.account_id, t.amount
  FROM transactions t
  JOIN transaction_current_status tcs
    ON tcs.transaction_id = t.id
  WHERE tcs.status = 'pending_policy'
  LIMIT 1
),
status_row AS (
  INSERT INTO transaction_status_history
    (transaction_id, status, tx_hash, block_number, created_at)
  SELECT tx.id, 'confirmed',
         '0x123456...abcdef', 12345678, NOW()
  FROM tx
  RETURNING transaction_id
),
jid AS (SELECT gen_random_uuid() AS val),
journal AS (
  INSERT INTO journal_entries
    (journal_id, account_id, coa_code, asset, debit, credit,
     entry_type, reference_id)
  SELECT jid.val, tx.account_id, 'WITHDRAWAL_PENDING', 'ETH',
         tx.amount, 0, 'withdrawal_settlement', tx.id
  FROM tx, jid
  UNION ALL
  SELECT jid.val, tx.account_id, 'HOT_WALLET', 'ETH',
         0, tx.amount, 'withdrawal_settlement', tx.id
  FROM tx, jid
  RETURNING id
),
outbox AS (
  INSERT INTO outbox_events
    (id, aggregate_id, event_type, payload, created_at)
  SELECT
    gen_random_uuid(), tx.id::VARCHAR,
    'withdrawal.confirmed',
    jsonb_build_object(
      'transaction_id', tx.id::VARCHAR,
      'tx_hash', '0x123456...abcdef',
      'block_number', '12345678',
      'amount', tx.amount::VARCHAR
    ),
    NOW()
  FROM tx
  RETURNING id
)
SELECT 'Withdrawal confirmed' AS result;

COMMIT;

-- -----------------------------------------------
-- STEP 5: Verify final state
-- Customer balance should still be 75 (reduced at initiation).
-- Pending obligation cleared. Hot wallet reduced by 25.
-- -----------------------------------------------

SELECT 'Final customer balance (should be 75)' AS step;
SELECT * FROM account_balances;

SELECT 'Transaction status (should be confirmed)' AS step;
SELECT * FROM transaction_current_status;

SELECT 'All journal entries' AS step;
SELECT
  je.journal_id,
  je.coa_code,
  je.debit,
  je.credit,
  je.entry_type,
  je.created_at
FROM journal_entries je
ORDER BY je.created_at, je.coa_code;

SELECT 'All outbox events' AS step;
SELECT * FROM outbox_events ORDER BY created_at;
