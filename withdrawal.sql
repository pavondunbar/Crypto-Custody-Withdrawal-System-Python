CREATE TABLE accounts(
  id UUID PRIMARY KEY,
  user_id UUID NOT NULL,
  asset VARCHAR(10) NOT NULL,
  balance DECIMAL(38,18) NOT NULL DEFAULT 0,
  locked_balance DECIMAL(38,18) NOT NULL DEFAULT 0,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL
);

CREATE TABLE transactions(
  id UUID PRIMARY KEY,
  account_id UUID REFERENCES accounts(id),
  type VARCHAR(20) NOT NULL,
  amount DECIMAL(38,18) NOT NULL,
  status VARCHAR(20) NOT NULL,
  destination_address VARCHAR(256),
  tx_hash VARCHAR(256),
  block_number BIGINT,
  idempotency_key VARCHAR(256) UNIQUE,
  policy_check_result JSONB,
  created_at TIMESTAMP NOT NULL,
  confirmed_at TIMESTAMP        -- NULL means in flight, NOT NULL means confirmed
);

CREATE TABLE outbox_events(
  id UUID PRIMARY KEY,
  aggregate_id VARCHAR(256) NOT NULL,
  event_type VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT NOW(),
  published_at TIMESTAMP        -- NULL means not yet delivered to Kafka
);

CREATE INDEX idx_transactions_status
ON transactions(status, created_at);

CREATE INDEX idx_outbox_unpublished
ON outbox_events(created_at)
WHERE published_at IS NULL;

-- -----------------------------------------------
-- SETUP
-- -----------------------------------------------

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- -----------------------------------------------
-- STEP 1: Insert account record
-- -----------------------------------------------

INSERT INTO accounts (id, user_id, asset, balance, locked_balance, created_at, updated_at)
VALUES (
    gen_random_uuid(),
    gen_random_uuid(),
    'ETH',
    100.000000000000000000,
    25.000000000000000000,
    NOW(),
    NOW()
);

SELECT * FROM accounts;

-- -----------------------------------------------
-- STEP 2: Atomic withdrawal initiation
-- Lock account, create transaction, write outbox
-- -----------------------------------------------

BEGIN;

WITH locked_account AS (
    SELECT id
    FROM accounts
    WHERE asset = 'ETH'
    FOR UPDATE
),

inserted_transaction AS (
    INSERT INTO transactions (
        id,
        account_id,
        type,
        amount,
        status,
        destination_address,
        idempotency_key,
        created_at
    )
    SELECT
        gen_random_uuid(),
        id,
        'withdrawal',
        25.000000000000000000,
        'pending_policy',
        'bc1qxy2kgdygjrsqtzq2n0yrf',
        gen_random_uuid()::VARCHAR,
        NOW()
    FROM locked_account
    RETURNING id, amount, account_id
),

inserted_event AS (
    INSERT INTO outbox_events (
        id,
        aggregate_id,
        event_type,
        payload,
        created_at
    )
    SELECT
        gen_random_uuid(),
        id::VARCHAR,
        'withdrawal.pending_policy',
        jsonb_build_object(
            'transaction_id', id::VARCHAR,
            'amount', amount::VARCHAR,
            'destination', '0xabcdef...123456'
        ),
        NOW()
    FROM inserted_transaction
    RETURNING id
)

SELECT 'Transaction and outbox event created successfully' AS result;

COMMIT;

-- Verify initiation state
SELECT * FROM accounts;
SELECT * FROM transactions;
SELECT * FROM outbox_events;

-- -----------------------------------------------
-- STEP 3: Blockchain confirmation
-- Simulates what ConfirmationTracker does after
-- the transaction is mined on chain
-- -----------------------------------------------

BEGIN;

-- Confirm the transaction — set tx_hash, block_number, confirmed_at
UPDATE transactions
SET
    tx_hash = '0x123456...abcdef',         -- hash returned from blockchain node
    block_number = 12345678,             -- block it was mined in
    status = 'confirmed',
    confirmed_at = NOW()                 -- NULL → meaningful timestamp
WHERE account_id = (SELECT id FROM accounts WHERE asset = 'ETH');

-- Settle the ledger — debit balance and release locked_balance atomically
-- These two updates ALWAYS happen together or not at all
UPDATE accounts
SET
    balance = balance - 25,              -- now actually deduct from balance
    locked_balance = locked_balance - 25, -- release the reserved funds
    updated_at = NOW()
WHERE asset = 'ETH';

-- Write confirmation event to outbox
-- Downstream services (notifications, reconciliation) consume this
INSERT INTO outbox_events (
    id,
    aggregate_id,
    event_type,
    payload,
    created_at
)
SELECT
    gen_random_uuid(),
    id::VARCHAR,
    'withdrawal.confirmed',
    jsonb_build_object(
        'transaction_id', id::VARCHAR,
        'tx_hash', '0x123456...abcdef',
        'block_number', '12345678',
        'amount', amount::VARCHAR
    ),
    NOW()
FROM transactions
WHERE status = 'confirmed';

COMMIT;

-- -----------------------------------------------
-- STEP 4: Verify final state
-- available = balance - locked_balance
-- Should be unchanged from customer perspective
-- -----------------------------------------------

SELECT
    a.balance,
    a.locked_balance,
    a.balance - a.locked_balance AS available,
    t.status,
    t.tx_hash,
    t.block_number,
    t.confirmed_at
FROM accounts a
JOIN transactions t ON t.account_id = a.id;

SELECT * FROM outbox_events ORDER BY created_at;
