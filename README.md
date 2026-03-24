# Crypto Custody Withdrawal System (Python)

<img width="1498" height="696" alt="Screenshot 2026-03-17 at 11 17 29 AM" src="https://github.com/user-attachments/assets/8001d144-6d60-4cd4-a52b-c36fbec0a108" />

> **SANDBOX / EDUCATIONAL USE ONLY --- NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not hardened, and must not be used to handle real funds or deployed to a production environment.** See the [Production Warning](#production-warning) section for full details.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Key Features](#key-features)
- [How It Works](#how-it-works)
- [Double-Entry Accounting Model](#double-entry-accounting-model)
- [Database Schema](#database-schema)
- [Transaction State Machine](#transaction-state-machine)
- [Running in a Sandbox Environment](#running-in-a-sandbox-environment)
- [Project Structure](#project-structure)
- [Production Warning](#production-warning)
- [License](#license)

---

## Overview

The **Crypto Custody Withdrawal System** is a Python-based reference implementation that models the core backend logic of a **custodial cryptocurrency withdrawal pipeline** using **double-entry accounting**. It demonstrates how a financial platform can safely accept, validate, process, and confirm crypto withdrawal requests while maintaining a fully auditable, append-only ledger where balances are derived from journal entries rather than stored as mutable fields.

This system is built around three tightly integrated components:

| Component | File | Responsibility |
|---|---|---|
| Withdrawal Service | `withdrawal.py` | Orchestrates the full withdrawal lifecycle with journal entries |
| Database Schema & Demo | `withdrawal.sql` | Double-entry schema, triggers, views, and runnable walkthrough |
| Outbox Publisher | `outbox-publisher.py` | Reliably delivers database events to Kafka |

---

## Architecture

```
                         +--------------------------------------+
                         |          WithdrawalService           |
                         |                                      |
  Client Request ------> |  1. Idempotency Check               |
                         |  2. Address Validation              |
                         |  3. Pessimistic Lock (account row)   |---> PostgreSQL
                         |  4. Derive Balance (journal view)    |
                         |  5. INSERT Transaction + Journal     |
                         |  6. Outbox Event (same transaction)  |
                         |  7. Policy Engine Evaluation         |---> PolicyEngine
                         |  8. Push to Signing Queue            |---> SQS FIFO Queue
                         +--------------------------------------+

                         +--------------------------------------+
                         |          OutboxPublisher             |
                         |                                      |
  PostgreSQL Outbox ---> |  Poll unpublished events             |---> Kafka Topics
   (background loop)     |  Mark published atomically           |
                         +--------------------------------------+

                         +--------------------------------------+
                         |       ConfirmationTracker            |
                         |                                      |
  Blockchain Node ------>|  Detect mined tx                     |---> PostgreSQL
                         |  INSERT settlement journal pair      |---> Outbox Event
                         |  INSERT status history (confirmed)   |
                         +--------------------------------------+
```

---

## Key Features

### Idempotency-Safe Withdrawals
Every withdrawal request carries an `idempotency_key`. The system checks for a matching key **before any database writes**, ensuring that retried or duplicate requests return the original result rather than creating duplicate transactions.

### Double-Entry Journal Ledger
Every balance-affecting operation records a balanced debit/credit pair in the `journal_entries` table. Balances are never stored as mutable fields --- they are derived from `SUM(credit) - SUM(debit)` via the `account_balances` view. A deferred constraint trigger rejects unbalanced entries at commit time.

### Append-Only Ledger Enforcement
`BEFORE UPDATE` and `BEFORE DELETE` triggers on `journal_entries`, `transactions`, and `transaction_status_history` raise exceptions on any mutation attempt. Once written, ledger data is immutable.

### Pessimistic Locking (Double-Spend Prevention)
The account row is locked with `SELECT ... FOR UPDATE` inside an atomic database transaction. The row serves as a lock target --- it contains no mutable balance fields.

### Transactional Outbox Pattern
The transaction record, journal entries, status history, and outbox event are all written in a **single atomic database transaction**. If the application crashes after writing but before publishing to Kafka, the outbox poller will still deliver the event.

### Reliable Event Delivery via Async Outbox Publisher
The `OutboxPublisher` runs as a background loop, polling `outbox_events` for undelivered messages. It uses `FOR UPDATE SKIP LOCKED` to allow multiple publisher replicas to run safely in parallel.

### Policy Engine Integration
Before a withdrawal is forwarded to signing, it is evaluated by an external **policy engine**. If rejected, a reversal journal pair restores the customer balance and a `REJECTED` status is appended to the history.

### FIFO Signing Queue with Per-Account Ordering
Approved withdrawals are published to a **FIFO message queue** using the account ID as the `MessageGroupId`, guaranteeing per-account ordering in the downstream signing service.

### Derived Balances via Views
The `account_balances` view computes balances from journal entries. The `transaction_current_status` view returns the latest status per transaction using `DISTINCT ON`. No mutable state is queried for balance checks.

---

## How It Works

### Withdrawal Initiation

Inside a single database transaction:
1. Lock the account row with `SELECT ... FOR UPDATE`
2. Derive the current balance from `account_balances` view
3. INSERT an immutable `transactions` row
4. INSERT a `PENDING_POLICY` row into `transaction_status_history`
5. INSERT a journal pair: DEBIT `CUST_LIABILITY`, CREDIT `WITHDRAWAL_PENDING`
6. INSERT a `withdrawal.pending_policy` outbox event

The customer's derived balance decreases immediately (funds "locked" in the pending account).

### Policy Evaluation

The policy engine is called **outside** the database transaction to avoid holding row locks during external calls. If rejected:
1. INSERT a reversal journal pair: DEBIT `WITHDRAWAL_PENDING`, CREDIT `CUST_LIABILITY`
2. INSERT a `REJECTED` status into `transaction_status_history`
3. INSERT a `withdrawal.rejected` outbox event

The customer's balance is restored.

### On-Chain Confirmation

When the blockchain transaction is confirmed:
1. INSERT a settlement journal pair: DEBIT `WITHDRAWAL_PENDING`, CREDIT `HOT_WALLET`
2. INSERT a `CONFIRMED` status with `tx_hash` and `block_number` into `transaction_status_history`
3. INSERT a `withdrawal.confirmed` outbox event

The pending obligation is cleared and hot wallet assets are reduced. The customer balance is unchanged (already reduced at initiation).

---

## Double-Entry Accounting Model

### Chart of Accounts

| Code | Name | Type | Normal Balance |
|---|---|---|---|
| `CUST_LIABILITY` | Customer Liability | Liability | Credit |
| `HOT_WALLET` | Hot Wallet | Asset | Debit |
| `WITHDRAWAL_PENDING` | Withdrawal Pending | Liability | Credit |
| `FEE_REVENUE` | Fee Revenue | Revenue | Credit |

### Journal Entry Flows

**Deposit:**

| Leg | COA Code | Debit | Credit |
|-----|----------|-------|--------|
| 1 | HOT_WALLET | amount | 0 |
| 2 | CUST_LIABILITY | 0 | amount |

**Withdrawal Initiation:**

| Leg | COA Code | Debit | Credit |
|-----|----------|-------|--------|
| 1 | CUST_LIABILITY | amount | 0 |
| 2 | WITHDRAWAL_PENDING | 0 | amount |

**Rejection (Reversal):**

| Leg | COA Code | Debit | Credit |
|-----|----------|-------|--------|
| 1 | WITHDRAWAL_PENDING | amount | 0 |
| 2 | CUST_LIABILITY | 0 | amount |

**Confirmation (Settlement):**

| Leg | COA Code | Debit | Credit |
|-----|----------|-------|--------|
| 1 | WITHDRAWAL_PENDING | amount | 0 |
| 2 | HOT_WALLET | 0 | amount |

---

## Database Schema

### `accounts`
Identity-only record. No mutable balance columns --- serves as a lock target for `SELECT ... FOR UPDATE`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `user_id` | UUID | Owning user |
| `asset` | VARCHAR(10) | e.g. `ETH`, `BTC` |
| `created_at` | TIMESTAMP | Row creation time |

### `chart_of_accounts`
Reference data for ledger categories.

| Column | Type | Description |
|---|---|---|
| `code` | VARCHAR(32) | Primary key (e.g. `CUST_LIABILITY`) |
| `name` | VARCHAR(128) | Human-readable name |
| `account_type` | VARCHAR(16) | `asset`, `liability`, or `revenue` |
| `normal_balance` | VARCHAR(8) | `debit` or `credit` |

### `journal_entries`
Append-only double-entry ledger. Each row is one leg of a balanced pair.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `journal_id` | UUID | Groups a balanced debit/credit pair |
| `account_id` | UUID | Foreign key to `accounts` |
| `coa_code` | VARCHAR(32) | Foreign key to `chart_of_accounts` |
| `asset` | VARCHAR(10) | Asset type |
| `debit` | DECIMAL(38,18) | Debit amount (0 if credit leg) |
| `credit` | DECIMAL(38,18) | Credit amount (0 if debit leg) |
| `entry_type` | VARCHAR(32) | e.g. `deposit`, `withdrawal_initiation` |
| `reference_id` | UUID | Transaction or entity this entry relates to |
| `created_at` | TIMESTAMP | Row creation time |

> CHECK constraint: each row must have exactly one of debit or credit > 0.

### `transactions`
Immutable transaction record. No `status`, `tx_hash`, or `block_number` columns.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `account_id` | UUID | Foreign key to `accounts` |
| `type` | VARCHAR(20) | Transaction type (e.g. `withdrawal`) |
| `amount` | DECIMAL(38,18) | Withdrawal amount |
| `destination_address` | VARCHAR(256) | Target crypto address |
| `idempotency_key` | VARCHAR(256) UNIQUE | Deduplication key |
| `policy_check_result` | JSONB | Result from policy engine evaluation |
| `created_at` | TIMESTAMP | Row creation time |

### `transaction_status_history`
Append-only status transitions. Latest row per `transaction_id` is the current status.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `transaction_id` | UUID | Foreign key to `transactions` |
| `status` | VARCHAR(20) | State machine value |
| `tx_hash` | VARCHAR(256) | On-chain hash (set on confirmation) |
| `block_number` | BIGINT | Block number (set on confirmation) |
| `metadata` | JSONB | Additional context |
| `created_at` | TIMESTAMP | Row creation time |

### `outbox_events`
Reliable event delivery buffer (unchanged).

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `aggregate_id` | VARCHAR(256) | Transaction ID (Kafka key / ordering) |
| `event_type` | VARCHAR(64) | e.g. `withdrawal.pending_policy` |
| `payload` | JSONB | Event data |
| `created_at` | TIMESTAMP | Row creation time (defaults to `NOW()`) |
| `published_at` | TIMESTAMP | NULL = pending delivery to Kafka |

### Views

- **`account_balances`** --- `SUM(credit) - SUM(debit)` from `journal_entries` where `coa_code = 'CUST_LIABILITY'`, grouped by `account_id` and `asset`.
- **`transaction_current_status`** --- `DISTINCT ON (transaction_id) ... ORDER BY created_at DESC` from `transaction_status_history`.

### Triggers

| Trigger | Table | Action | Purpose |
|---|---|---|---|
| `deny_update_journal` | `journal_entries` | BEFORE UPDATE | Append-only enforcement |
| `deny_delete_journal` | `journal_entries` | BEFORE DELETE | Append-only enforcement |
| `deny_update_transactions` | `transactions` | BEFORE UPDATE | Append-only enforcement |
| `deny_delete_transactions` | `transactions` | BEFORE DELETE | Append-only enforcement |
| `deny_update_status_history` | `transaction_status_history` | BEFORE UPDATE | Append-only enforcement |
| `deny_delete_status_history` | `transaction_status_history` | BEFORE DELETE | Append-only enforcement |
| `check_journal_balance` | `journal_entries` | AFTER INSERT (deferred) | Balanced journal pairs |

---

## Transaction State Machine

```
                     +------------------+
  New Request ------>|  PENDING_POLICY  |
                     +--------+---------+
                              |
               +--------------+--------------+
               v                             v
         +----------+                 +----------+
         | APPROVED |                 | REJECTED |
         +----+-----+                 +----------+
              |
              v
         +---------+
         | SIGNED  |
         +----+----+
              |
              v
         +-----------+
         | BROADCAST |
         +-----+-----+
               |
     +---------+----------+
     v                    v
+-----------+        +--------+
| CONFIRMED |        | FAILED |
+-----------+        +--------+
```

Status transitions are recorded as INSERT-only rows in `transaction_status_history`. The current status is the latest row per `transaction_id`.

---

## Running in a Sandbox Environment

> These instructions are for **local/sandbox use only**. No real assets are involved.

### Prerequisites

- Python 3.10+
- PostgreSQL 14+
- A Kafka instance (local or Docker)
- Optional: AWS SQS FIFO (or a local mock like ElasticMQ)

### 1. Clone the Repository

```bash
git clone https://github.com/pavondunbar/Crypto-Custody-Withdrawal-System-Python.git
cd Crypto-Custody-Withdrawal-System-Python
```

### 2. Create a Virtual Environment

```bash
python3 -m venv venv
source venv/bin/activate       # macOS/Linux
venv\Scripts\activate          # Windows
```

### 3. Install Dependencies

```bash
pip install asyncpg aiokafka
```

> `asyncio` is part of the Python standard library. Depending on your database adapter preference, you may also need `psycopg2-binary` to wire the synchronous `WithdrawalService` class to a real PostgreSQL connection.

### 4. Set Up PostgreSQL

Start a local PostgreSQL instance and create a sandbox database:

```bash
psql -U postgres -c "CREATE DATABASE custody;"
psql -U postgres -d custody -f withdrawal.sql
```

The SQL file will:
- Create all tables (accounts, chart_of_accounts, journal_entries, transactions, transaction_status_history, outbox_events)
- Create views (account_balances, transaction_current_status)
- Install append-only triggers and the journal balance constraint trigger
- Seed the chart of accounts
- Run a complete double-entry withdrawal lifecycle (deposit, initiation, confirmation)
- Print derived balances at each step for verification

### 5. Start a Local Kafka (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest
```

### 6. Run the Outbox Publisher

```bash
python outbox-publisher.py
```

It will poll `outbox_events` in a loop and deliver unpublished events to Kafka. Press `Ctrl+C` to stop gracefully.

### 7. Run the Withdrawal Demo

```bash
python withdrawal.py
```

The demo connects to `postgresql://postgres@localhost/custody`, seeds the chart of accounts, creates a test account, records a 10 ETH deposit via journal entry, and walks through the full withdrawal lifecycle with derived balance verification at each step.

### 8. Verify the Ledger

Run this query in psql to confirm derived balances:

```sql
SELECT * FROM account_balances;
```

To see the full journal:

```sql
SELECT
  journal_id,
  coa_code,
  debit,
  credit,
  entry_type,
  created_at
FROM journal_entries
ORDER BY created_at, coa_code;
```

To verify append-only enforcement:

```sql
-- Both of these should raise exceptions:
UPDATE journal_entries SET debit = 0 WHERE id = (SELECT id FROM journal_entries LIMIT 1);
DELETE FROM journal_entries WHERE id = (SELECT id FROM journal_entries LIMIT 1);
```

To verify balanced journal constraint:

```sql
-- This should fail at COMMIT:
BEGIN;
INSERT INTO journal_entries
  (journal_id, account_id, coa_code, asset, debit, credit,
   entry_type, reference_id)
VALUES
  (gen_random_uuid(),
   (SELECT id FROM accounts LIMIT 1),
   'HOT_WALLET', 'ETH', 50, 0,
   'test_unbalanced',
   gen_random_uuid());
COMMIT;
```

---

## Project Structure

```
Crypto-Custody-Withdrawal-System-Python/
|
|-- withdrawal.py          # WithdrawalService with double-entry journal entries
|-- withdrawal.sql         # PostgreSQL double-entry schema + demo walkthrough
|-- outbox-publisher.py    # Async background poller: DB outbox -> Kafka
+-- LICENSE                # MIT License
```

---

## Production Warning

**This project is explicitly NOT suitable for production use.** The following critical components are absent or stubbed:

| Missing Component | Risk if Absent |
|---|---|
| HSM / MPC key signing | Private keys would be exposed in software |
| Real address validation | Funds could be sent to invalid/malicious addresses |
| AML / KYC policy engine | Regulatory violations, sanctions exposure |
| Authentication & authorization | Any caller could initiate withdrawals |
| Rate limiting & withdrawal limits | Accounts could be drained rapidly |
| Secrets management | Database credentials exposed |
| Retry logic with dead-letter queues | Failed messages silently dropped |
| Security audit | Unknown vulnerabilities |
| Comprehensive test suite | Untested edge cases in fund handling |

> Handling real cryptocurrency requires engaging licensed custodians, security engineers, blockchain auditors, and legal counsel. **Do not use this code to hold, transfer, or manage real digital assets.**

---

## License

This project is licensed under the [MIT License](LICENSE).

---

*Built by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
