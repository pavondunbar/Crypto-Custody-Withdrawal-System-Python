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
- [Docker Services](#docker-services)
- [Project Structure](#project-structure)
- [Production Warning](#production-warning)
- [License](#license)

---

## Overview

The **Crypto Custody Withdrawal System** is a Python-based reference implementation that models the core backend logic of a **custodial cryptocurrency withdrawal pipeline** using **double-entry accounting**. It demonstrates how a financial platform can safely accept, validate, process, and confirm crypto withdrawal requests while maintaining a fully auditable, append-only ledger where balances are derived from journal entries rather than stored as mutable fields.

The system is composed of seven containerized services orchestrated via Docker Compose:

| Service | Directory | Responsibility |
|---|---|---|
| Withdrawal Service | `withdrawal/` | Orchestrates the full withdrawal lifecycle with journal entries |
| Outbox Publisher | `outbox/` | Reliably delivers database events to Kafka |
| Signing Gateway | `signing-gateway/` | Fan-out MPC signing orchestrator |
| MPC Nodes (x3) | `mpc/` | Threshold partial signature generation (stub) |
| PostgreSQL | `db/init/` | Double-entry schema, triggers, views, and seed data |
| Kafka + Zookeeper | (Docker images) | Event streaming infrastructure |

---

## Architecture

```
                         +--------------------------------------+
                         |          WithdrawalService           |
                         |                                      |
  Client Request ------> |  1. Idempotency Check               |
                         |  2. Address Validation              |
                         |  3. Pessimistic Lock (account row)   |---> PostgreSQL
                         |  4. Derive Balance (journal view)    |     (internal network)
                         |  5. INSERT Transaction + Journal     |
                         |  6. Outbox Event (same transaction)  |
                         |  7. Policy Engine Evaluation         |---> PolicyEngine
                         |  8. Push to Signing Queue            |---> SQS FIFO Queue
                         +--------------------------------------+

                         +--------------------------------------+
                         |          OutboxPublisher             |
                         |                                      |
  PostgreSQL Outbox ---> |  Poll unpublished events             |---> Kafka Topics
   (background loop)     |  Mark published atomically           |     (backend network)
                         |  (readonly DB user)                  |
                         +--------------------------------------+

                         +--------------------------------------+
                         |         SigningGateway                |
                         |                                      |
  Signing Request ------>|  Fan-out to MPC nodes (parallel)     |
                         |  Collect partial signatures          |---> MPC Nodes x3
                         |  Threshold check (majority)          |     (signing network)
                         |  Assemble combined signature         |
                         +--------------------------------------+

                         +--------------------------------------+
                         |       ConfirmationTracker            |
                         |                                      |
  Blockchain Node ------>|  Detect mined tx                     |---> PostgreSQL
                         |  INSERT settlement journal pair      |---> Outbox Event
                         |  INSERT status history (confirmed)   |
                         +--------------------------------------+
```

### Network Isolation

Three Docker networks enforce trust boundaries:

| Network | Visibility | Services |
|---|---|---|
| `backend` | Inter-service | Withdrawal, Outbox, Kafka, Zookeeper, Signing Gateway |
| `internal` | Database only | PostgreSQL, Withdrawal, Outbox |
| `signing` | MPC only | Signing Gateway, MPC Nodes 1-3 |

PostgreSQL is completely isolated from the host and from signing infrastructure. MPC nodes are unreachable from anything except the signing gateway.

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
The `OutboxPublisher` runs as a background loop, polling `outbox_events` for undelivered messages. It uses `FOR UPDATE SKIP LOCKED` to allow multiple publisher replicas to run safely in parallel. The publisher connects with a least-privilege database user that can only `SELECT` on `outbox_events` and `UPDATE` the `published_at` column.

### Policy Engine Integration
Before a withdrawal is forwarded to signing, it is evaluated by an external **policy engine**. If rejected, a reversal journal pair restores the customer balance and a `REJECTED` status is appended to the history.

### FIFO Signing Queue with Per-Account Ordering
Approved withdrawals are published to a **FIFO message queue** using the account ID as the `MessageGroupId`, guaranteeing per-account ordering in the downstream signing service.

### MPC Signing Gateway
A signing gateway fans out signing requests to three MPC nodes in parallel over an isolated Docker network. It collects partial signatures and requires a majority threshold (t-of-n) before assembling the combined signature. The current implementation uses deterministic stubs (SHA-256 hashes) rather than real threshold-ECDSA.

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
| `created_at` | TIMESTAMPTZ | Row creation time |

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
| `created_at` | TIMESTAMPTZ | Row creation time |

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
| `created_at` | TIMESTAMPTZ | Row creation time |

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
| `created_at` | TIMESTAMPTZ | Row creation time |

### `outbox_events`
Reliable event delivery buffer (unchanged).

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `aggregate_id` | VARCHAR(256) | Transaction ID (Kafka key / ordering) |
| `event_type` | VARCHAR(64) | e.g. `withdrawal.pending_policy` |
| `payload` | JSONB | Event data |
| `created_at` | TIMESTAMPTZ | Row creation time (defaults to `NOW()`) |
| `published_at` | TIMESTAMPTZ | NULL = pending delivery to Kafka |

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

- Docker and Docker Compose

### 1. Clone the Repository

```bash
git clone https://github.com/pavondunbar/CC-PYTHON.git
cd CC-PYTHON
```

### 2. Start All Services

```bash
docker-compose up -d
```

This starts all seven services: PostgreSQL, Zookeeper, Kafka, Withdrawal Service, Outbox Publisher, Signing Gateway, and three MPC nodes. The database schema is automatically initialized from `db/init/`.

### 3. Run the Withdrawal Demo

The withdrawal service runs its demo automatically on startup. Check the output:

```bash
docker-compose logs withdrawal-service
```

The demo seeds the chart of accounts, creates a test account, records a 10 ETH deposit via journal entries, and walks through the full withdrawal lifecycle with derived balance verification at each step.

To run the demo again:

```bash
docker-compose run withdrawal-service python withdrawal.py
```

### 4. Publish Outbox Events to Kafka

The outbox publisher runs continuously as a background poller. Check its status:

```bash
docker-compose logs outbox-publisher
```

### 5. Verify the Ledger

Query derived balances directly via the PostgreSQL container:

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db \
  -c "SELECT * FROM account_balances;"
```

To see the full journal:

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db -c "
  SELECT journal_id, coa_code, debit, credit, entry_type, created_at
  FROM journal_entries
  ORDER BY created_at, coa_code;
"
```

To verify append-only enforcement (both should raise exceptions):

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db -c "
  UPDATE journal_entries SET debit = 0
  WHERE id = (SELECT id FROM journal_entries LIMIT 1);
"
```

### 6. Inspect Kafka Topics

List all topics:

```bash
docker-compose exec kafka kafka-topics \
  --bootstrap-server localhost:9092 --list
```

Consume messages from a topic:

```bash
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic custody.withdrawal.confirmed \
  --from-beginning --timeout-ms 5000
```

### 7. Test the Signing Gateway

Send a signing request to the gateway:

```bash
docker-compose exec signing-gateway python -c "
import aiohttp, asyncio, json
async def test():
    async with aiohttp.ClientSession() as s:
        async with s.post('http://localhost:8000/sign',
            json={'tx_id': 'test-123', 'payload': 'deadbeef'}) as r:
            print(json.dumps(await r.json(), indent=2))
asyncio.run(test())
"
```

### 8. Clean Restart

To tear down everything and start fresh (removes database volume):

```bash
docker-compose down -v
docker-compose up -d
```

---

## Docker Services

| Container | Image | Networks | Role |
|---|---|---|---|
| `ledger-db` | postgres:15 | internal | Double-entry ledger database |
| `withdrawal-service` | ./withdrawal | backend, internal | Withdrawal lifecycle orchestration |
| `outbox-publisher` | ./outbox | backend, internal | Outbox event delivery to Kafka |
| `kafka` | cp-kafka:7.5.0 | backend | Event streaming broker |
| `zookeeper` | cp-zookeeper:7.5.0 | backend | Kafka coordination |
| `signing-gateway` | ./signing-gateway | backend, signing | Fan-out MPC signing orchestrator |
| `mpc-node-{1,2,3}` | ./mpc | signing | Threshold partial signature nodes |

All custom services use **Python 3.13-slim** base images. The database volume (`postgres_data`) persists data across restarts. Use `docker-compose down -v` to reset.

---

## Project Structure

```
CC-PYTHON/
├── db/
│   └── init/
│       ├── 001-schema.sql           # Double-entry schema, triggers, views, seed data
│       └── 002-readonly-user.sql    # Least-privilege DB user for outbox publisher
├── withdrawal/
│   ├── Dockerfile
│   └── withdrawal.py                # WithdrawalService with double-entry journal entries
├── outbox/
│   ├── Dockerfile
│   └── outbox-publisher.py          # Async background poller: DB outbox -> Kafka
├── signing-gateway/
│   ├── Dockerfile
│   └── gateway.py                   # Fan-out MPC signing orchestrator
├── mpc/
│   ├── Dockerfile
│   └── node.py                      # MPC node stub (deterministic signing)
├── docker-compose.yaml              # Full service orchestration (7 services, 3 networks)
└── LICENSE                          # MIT License
```

---

## Production Warning

**This project is explicitly NOT suitable for production use.** The following critical components are absent or stubbed:

| Missing Component | Risk if Absent |
|---|---|
| Real MPC / threshold-ECDSA | Signing gateway uses deterministic stubs, not real cryptography |
| Real address validation | Funds could be sent to invalid/malicious addresses |
| AML / KYC policy engine | Regulatory violations, sanctions exposure |
| Authentication & authorization | Any caller could initiate withdrawals |
| Rate limiting & withdrawal limits | Accounts could be drained rapidly |
| Secrets management | Database credentials passed via environment variables |
| Retry logic with dead-letter queues | Failed messages silently dropped |
| TLS / mTLS between services | Inter-service traffic is unencrypted |
| Security audit | Unknown vulnerabilities |
| Comprehensive test suite | Untested edge cases in fund handling |

> Handling real cryptocurrency requires engaging licensed custodians, security engineers, blockchain auditors, and legal counsel. **Do not use this code to hold, transfer, or manage real digital assets.**

---

## License

This project is licensed under the [MIT License](LICENSE).

---

*Built with ❤️ by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
