# Crypto Custody Withdrawal System

<img width="1536" height="1024" alt="4d927272-a067-4c9f-bc4c-814ea3e7d3a7" src="https://github.com/user-attachments/assets/29d5854e-05d5-42d8-a074-fd0d479c5f46" />

> **SANDBOX / EDUCATIONAL USE ONLY --- NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not hardened, and must not be used to handle real funds or deployed to a production environment.** See the [Production Warning](#production-warning) section for full details.

---

## Table of Contents

- [Overview](#overview)
- [Quick Start](#quick-start)
- [Architecture](#architecture)
- [Requirements Verification](#requirements-verification)
- [Key Features](#key-features)
- [How It Works](#how-it-works)
- [Double-Entry Accounting Model](#double-entry-accounting-model)
- [Database Schema](#database-schema)
- [Transaction State Machine](#transaction-state-machine)
- [Running in a Sandbox Environment](#running-in-a-sandbox-environment)
- [Testing](#testing)
- [Docker Services](#docker-services)
- [Project Structure](#project-structure)
- [Production Warning](#production-warning)
- [License](#license)

---

## Overview

The **Crypto Custody Withdrawal System** is a Python-based reference implementation that models the core backend logic of a **custodial cryptocurrency withdrawal pipeline** using **double-entry accounting**. It demonstrates how a financial platform can safely accept, validate, process, and confirm crypto withdrawal requests while maintaining a fully auditable, append-only ledger where balances are derived from journal entries rather than stored as mutable fields.

The system is composed of nine containerized services orchestrated via Docker Compose:

| Service | Directory | Responsibility |
|---|---|---|
| Withdrawal Service | `withdrawal/` | Orchestrates the full withdrawal lifecycle with journal entries and RBAC |
| Outbox Publisher | `outbox/` | Reliably delivers database events to Kafka with dead-letter queue support |
| Signing Gateway | `signing-gateway/` | Fan-out MPC signing orchestrator |
| MPC Nodes (x3) | `mpc/` | Threshold partial signature generation (stub) |
| Reconciliation Engine | `reconciliation/` | Replays journal entries and validates state machine integrity |
| Event Consumer | `event-consumer/` | Subscribes to Kafka topics and logs audit events |
| PostgreSQL | `db/init/` | Double-entry schema, triggers, views, RBAC, audit, and seed data |
| Kafka + Zookeeper | (Docker images) | Event streaming infrastructure |

---

## Quick Start

```bash
git clone https://github.com/pavondunbar/CC-PYTHON.git
cd CC-PYTHON
make up        # build and start all 9 services
make demo      # run the full withdrawal lifecycle demo
make demo-full # run end-to-end: withdrawal + Kafka + consumer
make logs      # follow logs from all services
make db-balances  # query derived balances from journal entries
make down      # tear down everything and remove volumes
```

All available Makefile targets:

| Command | Description |
|---|---|
| `make up` | Build and start all services |
| `make down` | Stop all services and remove volumes |
| `make demo` | Run the withdrawal demo (full state machine walkthrough) |
| `make demo-full` | Run full end-to-end demo (all services + Kafka publishing) |
| `make logs` | Follow logs from all services |
| `make db-balances` | Show derived account balances from journal entries |
| `make db-journal` | Show all journal entries |
| `make db-audit` | Show audit trail |
| `make db-recon` | Show reconciliation run history |
| `make status` | Show running containers |
| `make test` | Run core tests (no Docker required) |
| `make clean` | Remove `__pycache__` and `.pyc` files |
| `make help` | Show all targets with descriptions |

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
                         |  Retry with dead-letter queue        |
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
                         |          EventConsumer               |
                         |                                      |
  Kafka Topics --------->|  Subscribe to custody.withdrawal.*   |
                         |  Log audit events to PostgreSQL      |---> PostgreSQL
                         |  Forward approved txns to signing    |---> SigningGateway
                         +--------------------------------------+

                         +--------------------------------------+
                         |       ReconciliationEngine           |
                         |                                      |
  Periodic (cron) ------>|  Replay journal entries              |---> PostgreSQL
                         |  Compare to account_balances view    |     (internal network)
                         |  Validate state machine transitions  |
                         |  Record mismatches                   |
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
| `backend` | Inter-service | Withdrawal, Outbox, Kafka, Zookeeper, Signing Gateway, Event Consumer |
| `internal` | Database only | PostgreSQL, Withdrawal, Outbox, Reconciliation, Event Consumer |
| `signing` | MPC only | Signing Gateway, MPC Nodes 1-3 |

PostgreSQL is completely isolated from the host and from signing infrastructure. MPC nodes are unreachable from anything except the signing gateway.

---

## Requirements Verification

The following table maps each architectural requirement to its implementation in the codebase.

| # | Requirement | Status | Implementation |
|---|---|---|---|
| 1 | Real microservice architecture with Docker, clear trust boundaries and service isolation | Satisfied | 9 services in `docker-compose.yaml` across 3 isolated Docker networks (`backend`, `internal`, `signing`). PostgreSQL is unreachable from signing infrastructure. MPC nodes are unreachable except from the signing gateway. |
| 2 | Idempotency layer to prevent duplicate processing across APIs and message consumers | Satisfied | `UNIQUE` constraint on `transactions.idempotency_key`. Kafka producer uses `enable_idempotence=True`. Outbox publisher uses `FOR UPDATE SKIP LOCKED` for safe parallel polling. |
| 3 | Comprehensive audit trails with request_id / trace_id / actor metadata for all state transitions | Satisfied | `audit_events` table with `request_id`, `trace_id`, `actor_role`, `actor_id`, `action`, `resource_type`, `resource_id`, and `metadata` columns. Every status transition and withdrawal action inserts an audit event. Event consumer writes audit events for all Kafka messages. |
| 4 | Reconciliation engine (replay ledger, recompute balances, compare, alert on mismatch) | Satisfied | `reconciliation/reconciler.py` implements `replay_journal_balances()` -> `get_view_balances()` -> compare -> record mismatches in `reconciliation_mismatches`. Supports `reconcile` (balance only) and `rebuild` (full state validation) modes. |
| 5 | Postgres-based ledger: append-only, double-entry, balance-derived, immutable | Satisfied | `journal_entries` uses debit/credit pairs with a deferred constraint trigger (`check_journal_balance`) that rejects unbalanced entries at commit. `account_balances` view derives balances via `SUM(credit) - SUM(debit)`. `BEFORE UPDATE` and `BEFORE DELETE` triggers on `journal_entries`, `transactions`, and `transaction_status_history` raise exceptions on mutation. |
| 6 | Kafka-based messaging: outbox pattern, at-least-once delivery, DLQ | Satisfied | `outbox/outbox-publisher.py` polls `outbox_events` and publishes to Kafka topics (`custody.withdrawal.*`). Events retry up to 5 times before moving to `dead_letter_queue`. Kafka producer uses `acks="all"` for durability. |
| 7 | Outbox publisher atomically persists events with ledger writes and asynchronously publishes | Satisfied | Journal entries, status history, and outbox events are inserted in a single `async with conn.transaction()` block. The outbox publisher runs as a separate service that polls and publishes asynchronously. |
| 8 | RBAC enforcing separation of duties (Admin, System, Signer) | Satisfied | `roles` table seeded with `admin` (approve_withdrawal, view_transactions), `system` (publish_events, reconcile), and `signer` (sign_transaction). Python-side enforcement in `withdrawal/rbac.py`. No single role has both approve and sign permissions. |
| 9 | Deterministic settlement state machine (PENDING -> APPROVED -> SIGNED -> BROADCASTED -> CONFIRMED) | Satisfied | `VALID_TRANSITIONS` dict in `rbac.py` + `enforce_status_transition` database trigger. Terminal states (`rejected`, `confirmed`, `failed`) block further transitions. Status history is append-only. |
| 10 | MPC-based transaction signing (2-of-3 quorum) with simulated hashes | Satisfied | `signing-gateway/gateway.py` fans out to 3 MPC nodes in parallel, requires majority threshold `(n // 2) + 1 = 2`. Each node in `mpc/node.py` returns a deterministic SHA-256 partial signature. Combined stub signature returned on quorum. |
| 11 | Deterministic state rebuild capability (full system state reconstructed from ledger) | Satisfied | `reconciler.py` `run_state_rebuild()` replays `transaction_status_history` in chronological order per transaction, validates each transition against `VALID_TRANSITIONS`, and replays journal entries to recompute all balances. |

---

## Key Features

### Idempotency-Safe Withdrawals
Every withdrawal request carries an `idempotency_key`. The system checks for a matching key **before any database writes**, ensuring that retried or duplicate requests return the original result rather than creating duplicate transactions.

### Double-Entry Journal Ledger
Every balance-affecting operation records a balanced debit/credit pair in the `journal_entries` table. Balances are never stored as mutable fields --- they are derived from `SUM(credit) - SUM(debit)` via the `account_balances` view. A deferred constraint trigger rejects unbalanced entries at commit time.

### Append-Only Ledger Enforcement
`BEFORE UPDATE` and `BEFORE DELETE` triggers on `journal_entries`, `transactions`, `transaction_status_history`, and `audit_events` raise exceptions on any mutation attempt. Once written, ledger data is immutable.

### Pessimistic Locking (Double-Spend Prevention)
The account row is locked with `SELECT ... FOR UPDATE` inside an atomic database transaction. The row serves as a lock target --- it contains no mutable balance fields.

### Transactional Outbox Pattern
The transaction record, journal entries, status history, and outbox event are all written in a **single atomic database transaction**. If the application crashes after writing but before publishing to Kafka, the outbox poller will still deliver the event.

### Reliable Event Delivery via Async Outbox Publisher
The `OutboxPublisher` runs as a background loop, polling `outbox_events` for undelivered messages. It uses `FOR UPDATE SKIP LOCKED` to allow multiple publisher replicas to run safely in parallel. The publisher connects with a least-privilege database user. Failed events are retried up to 5 times before being moved to a **dead-letter queue** for manual investigation.

### Policy Engine Integration
Before a withdrawal is forwarded to signing, it is evaluated by an external **policy engine**. If rejected, a reversal journal pair restores the customer balance and a `REJECTED` status is appended to the history.

### FIFO Signing Queue with Per-Account Ordering
Approved withdrawals are published to a **FIFO message queue** using the account ID as the `MessageGroupId`, guaranteeing per-account ordering in the downstream signing service.

### MPC Signing Gateway
A signing gateway fans out signing requests to three MPC nodes in parallel over an isolated Docker network. It collects partial signatures and requires a majority threshold (t-of-n) before assembling the combined signature. The current implementation uses deterministic stubs (SHA-256 hashes) rather than real threshold-ECDSA.

### Derived Balances via Views
The `account_balances` view computes balances from journal entries. The `transaction_current_status` view returns the latest status per transaction using `DISTINCT ON`. No mutable state is queried for balance checks.

### Role-Based Access Control (RBAC)
Roles (`admin`, `system`, `signer`) are defined in the `roles` table with JSONB permission arrays. Python-side RBAC utilities in `withdrawal/rbac.py` enforce permission checks before actions like approving withdrawals, publishing events, or signing transactions.

### State Machine Enforcement at Database Level
Valid status transitions are enforced both in Python (defense-in-depth) and at the database layer via a `BEFORE INSERT` trigger on `transaction_status_history`. Terminal states (`rejected`, `confirmed`, `failed`) block further transitions.

### Audit Trail
Every significant action is logged to an append-only `audit_events` table with `trace_id` correlation, `actor_role`, and `action` fields. The event consumer subscribes to all Kafka withdrawal topics and writes audit events for observability and compliance.

### Reconciliation Engine
A dedicated reconciliation service replays journal entries and compares derived balances against the `account_balances` view. It supports two modes:
- **reconcile**: Balance verification only
- **rebuild**: Full state machine validation plus balance verification

Mismatches are recorded in `reconciliation_mismatches` and run history is tracked in `reconciliation_runs`.

### Dead-Letter Queue
Events that fail delivery after 5 retries are moved to a `dead_letter_queue` table. DLQ entries track retry counts, error messages, and resolution status. Database triggers enforce controlled updates (only `retry_count`, `last_retry_at`, and `resolved_at` may be modified).

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
7. INSERT an audit event with trace ID

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

### Core Tables

#### `accounts`
Identity-only record. No mutable balance columns --- serves as a lock target for `SELECT ... FOR UPDATE`.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `user_id` | UUID | Owning user |
| `asset` | VARCHAR(10) | e.g. `ETH`, `BTC` |
| `created_at` | TIMESTAMPTZ | Row creation time |

#### `chart_of_accounts`
Reference data for ledger categories.

| Column | Type | Description |
|---|---|---|
| `code` | VARCHAR(32) | Primary key (e.g. `CUST_LIABILITY`) |
| `name` | VARCHAR(128) | Human-readable name |
| `account_type` | VARCHAR(16) | `asset`, `liability`, or `revenue` |
| `normal_balance` | VARCHAR(8) | `debit` or `credit` |

#### `journal_entries`
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

#### `transactions`
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

#### `transaction_status_history`
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

#### `outbox_events`
Reliable event delivery buffer.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `aggregate_id` | VARCHAR(256) | Transaction ID (Kafka key / ordering) |
| `event_type` | VARCHAR(64) | e.g. `withdrawal.pending_policy` |
| `payload` | JSONB | Event data |
| `created_at` | TIMESTAMPTZ | Row creation time (defaults to `NOW()`) |
| `published_at` | TIMESTAMPTZ | NULL = pending delivery to Kafka |

### Audit & RBAC Tables

#### `audit_events`
Append-only audit trail for compliance and observability.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `request_id` | UUID | Originating request identifier |
| `trace_id` | UUID | Correlation ID linking related events |
| `actor_role` | VARCHAR(32) | `admin`, `system`, or `signer` |
| `actor_id` | VARCHAR(128) | Identity of the acting entity |
| `action` | VARCHAR(64) | Action performed (e.g. `initiate_withdrawal`) |
| `resource_type` | VARCHAR(64) | Type of resource affected |
| `resource_id` | UUID | Identifier of the affected resource |
| `metadata` | JSONB | Additional context |
| `created_at` | TIMESTAMPTZ | Row creation time |

> Indexed on `(trace_id)` and `(resource_type, resource_id)`. Append-only via triggers.

#### `roles`
RBAC role definitions with permission arrays.

| Column | Type | Description |
|---|---|---|
| `code` | VARCHAR(32) | Primary key (e.g. `admin`, `system`, `signer`) |
| `name` | VARCHAR(128) | Human-readable role name |
| `permissions` | JSONB | Array of allowed actions |

Seeded roles:
- **admin**: `approve_withdrawal`, `view_transactions`
- **system**: `publish_events`, `reconcile`
- **signer**: `sign_transaction`

### Reconciliation Tables

#### `reconciliation_runs`
Tracks reconciliation execution history.

| Column | Type | Description |
|---|---|---|
| `run_id` | UUID | Primary key |
| `started_at` | TIMESTAMPTZ | Run start time |
| `completed_at` | TIMESTAMPTZ | Run completion time |
| `status` | VARCHAR(16) | `running`, `passed`, `failed`, or `error` |
| `total_accounts` | INT | Number of accounts checked |
| `mismatches_found` | INT | Number of balance discrepancies |

> Update guard: status can only transition from `running` to a terminal state. Deletes are blocked.

#### `reconciliation_mismatches`
Records balance discrepancies found during reconciliation.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `run_id` | UUID | Foreign key to `reconciliation_runs` |
| `account_id` | UUID | Foreign key to `accounts` |
| `asset` | VARCHAR(10) | Asset type |
| `expected_balance` | DECIMAL(38,18) | Balance from journal replay |
| `actual_balance` | DECIMAL(38,18) | Balance from view |
| `difference` | DECIMAL(38,18) | Discrepancy amount |
| `created_at` | TIMESTAMPTZ | Row creation time |

> Append-only via triggers.

### Dead-Letter Queue

#### `dead_letter_queue`
Stores events that failed delivery after maximum retries.

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `original_event_id` | UUID | Foreign key to `outbox_events` |
| `error_message` | TEXT | Failure description |
| `retry_count` | INT | Number of delivery attempts |
| `max_retries` | INT | Maximum allowed retries |
| `last_retry_at` | TIMESTAMPTZ | Most recent retry timestamp |
| `resolved_at` | TIMESTAMPTZ | NULL = unresolved |
| `created_at` | TIMESTAMPTZ | Row creation time |

> Deletes are blocked. Updates are restricted to `retry_count`, `last_retry_at`, and `resolved_at` only. Indexed on `(created_at) WHERE resolved_at IS NULL`.

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
| `enforce_status_transition` | `transaction_status_history` | BEFORE INSERT | State machine validation |
| `deny_update_audit` | `audit_events` | BEFORE UPDATE | Append-only enforcement |
| `deny_delete_audit` | `audit_events` | BEFORE DELETE | Append-only enforcement |
| `deny_delete_recon_runs` | `reconciliation_runs` | BEFORE DELETE | Delete prevention |
| `guard_recon_run_update` | `reconciliation_runs` | BEFORE UPDATE | Only running -> terminal allowed |
| `deny_update_recon_mismatches` | `reconciliation_mismatches` | BEFORE UPDATE | Append-only enforcement |
| `deny_delete_recon_mismatches` | `reconciliation_mismatches` | BEFORE DELETE | Append-only enforcement |
| `deny_delete_dlq` | `dead_letter_queue` | BEFORE DELETE | Delete prevention |
| `guard_dlq_update` | `dead_letter_queue` | BEFORE UPDATE | Controlled field updates only |

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
         | SIGNED  |------> FAILED
         +----+----+
              |
              v
         +-----------+
         | BROADCAST |-----> FAILED
         +-----+-----+
               |
               v
         +-----------+
         | CONFIRMED |
         +-----------+
```

Valid transitions enforced at the database level:

| From | Allowed Targets |
|---|---|
| *(none)* | `pending_policy` |
| `pending_policy` | `approved`, `rejected` |
| `approved` | `signed`, `failed` |
| `signed` | `broadcast`, `failed` |
| `broadcast` | `confirmed`, `failed` |
| `rejected` | *(terminal)* |
| `confirmed` | *(terminal)* |
| `failed` | *(terminal)* |

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
make up
```

This starts all nine services: PostgreSQL, Zookeeper, Kafka, Withdrawal Service, Outbox Publisher, Signing Gateway, three MPC nodes, Reconciliation Engine, and Event Consumer. The database schema is automatically initialized from `db/init/`.

### 3. Run the Withdrawal Demo

The withdrawal service runs its demo automatically on startup. Check the output:

```bash
docker-compose logs withdrawal-service
```

The demo seeds the chart of accounts, creates a test account, records a 10 ETH deposit via journal entries, and walks through the full withdrawal lifecycle with derived balance verification and audit trail at each step.

To run the demo again:

```bash
make demo
```

### 4. Publish Outbox Events to Kafka

The outbox publisher runs continuously as a background poller. Check its status:

```bash
docker-compose logs outbox-publisher
```

### 5. Monitor the Event Consumer

The event consumer subscribes to all `custody.withdrawal.*` Kafka topics and writes audit events:

```bash
docker-compose logs event-consumer
```

### 6. Check Reconciliation

The reconciliation engine runs periodically (default: every 60 seconds). Check its output:

```bash
docker-compose logs reconciliation
```

### 7. Verify the Ledger

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

To verify append-only enforcement (should raise an exception):

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db -c "
  UPDATE journal_entries SET debit = 0
  WHERE id = (SELECT id FROM journal_entries LIMIT 1);
"
```

To view the audit trail:

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db -c "
  SELECT trace_id, actor_role, action, resource_type, created_at
  FROM audit_events
  ORDER BY created_at;
"
```

To check reconciliation history:

```bash
docker-compose exec postgres psql -U ledger_user -d ledger_db -c "
  SELECT run_id, status, total_accounts, mismatches_found, started_at
  FROM reconciliation_runs
  ORDER BY started_at DESC;
"
```

### 8. Inspect Kafka Topics

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

### 9. Test the Signing Gateway

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

### 10. Clean Restart

To tear down everything and start fresh (removes database volume):

```bash
make down
make up
```

---

## Testing

The project includes a core test suite in `tests/test_core.py` that validates the most critical paths without requiring Docker or a running database. Tests cover:

- **State machine valid transitions**: Full happy path from `PENDING_POLICY` through `CONFIRMED`, plus rejection and failure branches
- **State machine invalid transitions**: Verifies that skipping states, reversing transitions, and escaping terminal states all raise `ValueError`
- **RBAC enforcement**: Confirms each role has the correct permissions and that separation of duties is maintained (no role has both `approve_withdrawal` and `sign_transaction`)
- **Schema immutability**: Parses the SQL schema files to verify that all append-only tables have `BEFORE UPDATE` and `BEFORE DELETE` triggers, that the deferred journal balance trigger exists, and that the state machine enforcement trigger exists
- **Grant safety**: Confirms the schema does not grant `UPDATE` or `DELETE` on `journal_entries`

### Running Tests

```bash
make test
```

Or directly:

```bash
python -m pytest tests/ -v
```

37 tests, all passing:

```
tests/test_core.py::TestStateMachineValid       (9 tests)
tests/test_core.py::TestStateMachineInvalid      (8 tests)
tests/test_core.py::TestRBAC                     (7 tests)
tests/test_core.py::TestSchemaConstraints        (13 tests)
```

---

## Docker Services

| Container | Image | Networks | Role |
|---|---|---|---|
| `ledger-db` | postgres:15 | internal | Double-entry ledger database |
| `withdrawal-service` | ./withdrawal | backend, internal | Withdrawal lifecycle orchestration with RBAC |
| `outbox-publisher` | ./outbox | backend, internal | Outbox event delivery to Kafka with DLQ |
| `kafka` | cp-kafka:7.5.0 | backend | Event streaming broker |
| `zookeeper` | cp-zookeeper:7.5.0 | backend | Kafka coordination |
| `signing-gateway` | ./signing-gateway | backend, signing | Fan-out MPC signing orchestrator |
| `mpc-node-{1,2,3}` | ./mpc | signing | Threshold partial signature nodes |
| `reconciliation` | ./reconciliation | internal | Periodic ledger and state machine auditing |
| `event-consumer` | ./event-consumer | backend, internal | Kafka event subscriber and audit logger |

All custom services use **Python 3.13-slim** base images. The database volume (`postgres_data`) persists data across restarts. Use `docker-compose down -v` to reset.

---

## Project Structure

```
CC-PYTHON/
├── db/
│   └── init/
│       ├── 001-schema.sql              # Double-entry schema, triggers, views, seed data
│       ├── 002-readonly-user.sql       # Least-privilege DB user for outbox publisher
│       ├── 003-missing-components.sql  # Audit, RBAC, reconciliation, DLQ tables
│       └── 004-extended-grants.sql     # Extended permissions for readonly_user
├── withdrawal/
│   ├── Dockerfile
│   ├── withdrawal.py                   # WithdrawalService with double-entry journal entries
│   ├── rbac.py                         # RBAC utilities and state machine validation
│   └── withdrawal.sql                  # SQL demo walkthrough
├── outbox/
│   ├── Dockerfile
│   └── outbox-publisher.py             # Async background poller: DB outbox -> Kafka with DLQ
├── signing-gateway/
│   ├── Dockerfile
│   └── gateway.py                      # Fan-out MPC signing orchestrator
├── mpc/
│   ├── Dockerfile
│   └── node.py                         # MPC node stub (deterministic signing)
├── reconciliation/
│   ├── Dockerfile
│   └── reconciler.py                   # Journal replay and state machine validation
├── event-consumer/
│   ├── Dockerfile
│   └── consumer.py                     # Kafka subscriber with audit event logging
├── tests/
│   └── test_core.py                    # Core tests: state machine, RBAC, schema constraints
├── docker-compose.yaml                 # Full service orchestration (9 services, 3 networks)
├── Makefile                            # make up, make down, make demo, make demo-full, make test
├── .gitignore
└── LICENSE                             # MIT License
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
| TLS / mTLS between services | Inter-service traffic is unencrypted |
| Security audit | Unknown vulnerabilities |
| Full integration test suite | Core tests cover state machine and RBAC; integration tests for fund handling are absent |

> Handling real cryptocurrency requires engaging licensed custodians, security engineers, blockchain auditors, and legal counsel. **Do not use this code to hold, transfer, or manage real digital assets.**

---

## License

This project is licensed under the [MIT License](LICENSE).

---

*Built by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
