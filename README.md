# 🔐 Crypto Custody Withdrawal System (Python)

<img width="1498" height="696" alt="Screenshot 2026-03-17 at 11 17 29 AM" src="https://github.com/user-attachments/assets/8001d144-6d60-4cd4-a52b-c36fbec0a108" />

> ⚠️ **SANDBOX / EDUCATIONAL USE ONLY — NOT FOR PRODUCTION**
> This codebase is a reference implementation designed for learning, prototyping, and architectural exploration. It is **not audited, not hardened, and must not be used to handle real funds or deployed to a production environment.** See the [Production Warning](#-production-warning) section for full details.

---

## Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Key Features](#-key-features)
- [How It Works](#-how-it-works)
- [Database Schema](#-database-schema)
- [Transaction State Machine](#-transaction-state-machine)
- [Running in a Sandbox Environment](#-running-in-a-sandbox-environment)
- [Project Structure](#-project-structure)
- [Production Warning](#-production-warning)
- [License](#-license)

---

## 📖 Overview

The **Crypto Custody Withdrawal System** is a Python-based reference implementation that models the core backend logic of a **custodial cryptocurrency withdrawal pipeline**. It demonstrates how a financial platform can safely accept, validate, process, and confirm crypto withdrawal requests while protecting against common failure modes like double-spends, race conditions, duplicate requests, and message delivery loss.

This system is built around three tightly integrated components:

| Component | File | Responsibility |
|---|---|---|
| Withdrawal Service | `withdrawal.py` | Orchestrates the full withdrawal lifecycle |
| Database Schema & Demo | `withdrawal.sql` | PostgreSQL schema, indexes, and runnable walkthrough |
| Outbox Publisher | `outbox-publisher.py` | Reliably delivers database events to Kafka |

---

## 🏗 Architecture

```
                         ┌──────────────────────────────────────┐
                         │          WithdrawalService           │
                         │                                      │
  Client Request ──────► │  1. Idempotency Check               │
                         │  2. Address Validation              │
                         │  3. Pessimistic Lock + Fund Reserve  │──► PostgreSQL
                         │  4. Outbox Event (same transaction)  │
                         │  5. Policy Engine Evaluation         │──► PolicyEngine
                         │  6. Push to Signing Queue            │──► SQS FIFO Queue
                         └──────────────────────────────────────┘

                         ┌──────────────────────────────────────┐
                         │          OutboxPublisher             │
                         │                                      │
  PostgreSQL Outbox ───► │  Poll unpublished events             │──► Kafka Topics
   (background loop)     │  Mark published atomically           │
                         └──────────────────────────────────────┘

                         ┌──────────────────────────────────────┐
                         │       ConfirmationTracker            │
                         │    (referenced in SQL demo)          │
  Blockchain Node ──────►│  Detect mined tx, update status      │──► PostgreSQL
                         │  Settle ledger atomically            │──► Outbox Event
                         └──────────────────────────────────────┘
```

---

## ✨ Key Features

### ✅ Idempotency-Safe Withdrawals
Every withdrawal request carries an `idempotency_key`. The system checks for a matching key **before any database writes**, ensuring that retried or duplicate requests return the original result rather than creating duplicate transactions. This is critical in financial systems where network failures can cause clients to retry.

### ✅ Pessimistic Locking (Double-Spend Prevention)
Funds are reserved using a `SELECT ... FOR UPDATE` row-level lock inside an atomic database transaction. This means two concurrent withdrawal attempts on the same account cannot both succeed — the second request will block until the first completes, then see the updated (reduced) available balance.

### ✅ Transactional Outbox Pattern
The withdrawal record and its corresponding outbox event are written in a **single atomic database transaction**. This eliminates the dual-write problem: if the application crashes after writing the transaction but before publishing to Kafka, the outbox poller will still deliver the event. Events are never lost and never duplicated.

### ✅ Reliable Event Delivery via Async Outbox Publisher
The `OutboxPublisher` runs as a background loop, polling `outbox_events` for undelivered messages. It uses `FOR UPDATE SKIP LOCKED` to allow multiple publisher replicas to run safely in parallel without stepping on each other. Each event is marked `published_at` only after successful delivery to Kafka.

### ✅ Policy Engine Integration
Before a withdrawal is forwarded to signing, it is evaluated by an external **policy engine**. If the policy decision is `rejected` (e.g., the destination address is on a blocklist, or the amount exceeds a daily limit), the locked funds are atomically released and the transaction is marked `REJECTED`. The policy check is intentionally placed **outside** the database transaction to avoid holding row locks during slow external calls.

### ✅ FIFO Signing Queue with Per-Account Ordering
Approved withdrawals are published to a **FIFO message queue** (e.g., AWS SQS FIFO) using the account ID as the `MessageGroupId`. This guarantees that withdrawals from the same account are processed strictly in order, preventing race conditions in the downstream signing service.

### ✅ Atomic Ledger Settlement on Confirmation
When a transaction is confirmed on-chain (simulated in `withdrawal.sql`), the balance deduction and locked-balance release happen in a single transaction. This ensures the ledger is never left in an inconsistent state — funds are never double-counted as both locked and deducted.

### ✅ Comprehensive Audit Trail
Every state transition emits an outbox event (e.g., `withdrawal.pending_policy`, `withdrawal.confirmed`). Downstream services such as notification systems, compliance tools, and reconciliation pipelines consume these events from Kafka, decoupled from the core withdrawal flow.

---

## 🔄 How It Works

### Step-by-Step Withdrawal Flow

**1. Idempotency Check**
The service queries the `transactions` table for an existing row matching the provided `idempotency_key`. If found, it immediately returns the existing transaction — no side effects.

**2. Address Validation**
The destination address is validated for the given asset type. An `InvalidAddressError` is raised if the address is malformed or empty.

**3. Atomic Fund Lock + Record Creation**
Inside a single database transaction:
- The account row is locked with `SELECT ... FOR UPDATE`
- Available balance (`balance - locked_balance`) is checked against the requested amount
- `locked_balance` is incremented by the withdrawal amount
- A transaction record is inserted with status `pending_policy`
- An outbox event (`withdrawal.pending_policy`) is written to the same transaction

**4. Policy Evaluation**
The policy engine is called outside the transaction. If rejected, `_refund_and_reject()` releases the lock and marks the transaction `REJECTED`.

**5. Signing Queue**
The transaction is published to a FIFO queue with per-account ordering. The downstream signing service picks it up, constructs and broadcasts the blockchain transaction.

**6. On-Chain Confirmation**
The `ConfirmationTracker` (shown in `withdrawal.sql`) monitors the blockchain, then atomically: sets `tx_hash`, `block_number`, `status = confirmed`, deducts from `balance`, releases `locked_balance`, and writes a `withdrawal.confirmed` outbox event.

---

## 🗄 Database Schema

### `accounts`
Tracks user balances with a two-field balance model:

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `user_id` | UUID | Owning user |
| `asset` | VARCHAR(10) | e.g. `ETH`, `BTC` |
| `balance` | DECIMAL(38,18) | Total balance |
| `locked_balance` | DECIMAL(38,18) | Reserved-but-not-yet-sent funds |
| `created_at` | TIMESTAMP | Row creation time |
| `updated_at` | TIMESTAMP | Last modification time |

> **Available balance** = `balance - locked_balance`

### `transactions`
Full lifecycle record for every withdrawal:

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `account_id` | UUID | Foreign key to `accounts` |
| `type` | VARCHAR(20) | Transaction type (e.g. `withdrawal`) |
| `amount` | DECIMAL(38,18) | Withdrawal amount |
| `status` | VARCHAR(20) | State machine value |
| `destination_address` | VARCHAR(256) | Target crypto address |
| `tx_hash` | VARCHAR(256) | On-chain transaction hash (set on confirmation) |
| `block_number` | BIGINT | Block number where the transaction was mined |
| `idempotency_key` | VARCHAR(256) UNIQUE | Deduplication key |
| `policy_check_result` | JSONB | Result from policy engine evaluation |
| `created_at` | TIMESTAMP | Row creation time |
| `confirmed_at` | TIMESTAMP | NULL = in-flight; NOT NULL = settled |

### `outbox_events`
Reliable event delivery buffer:

| Column | Type | Description |
|---|---|---|
| `id` | UUID | Primary key |
| `aggregate_id` | VARCHAR(256) | Transaction ID (used for Kafka key / ordering) |
| `event_type` | VARCHAR(64) | e.g. `withdrawal.pending_policy` |
| `payload` | JSONB | Event data |
| `created_at` | TIMESTAMP | Row creation time (defaults to `NOW()`) |
| `published_at` | TIMESTAMP | NULL = pending delivery to Kafka |

---

## 🔁 Transaction State Machine

```
                     ┌──────────────────┐
  New Request ──────►│  PENDING_POLICY  │
                     └────────┬─────────┘
                              │
               ┌──────────────┴──────────────┐
               ▼                             ▼
         ┌──────────┐                 ┌──────────┐
         │ APPROVED │                 │ REJECTED │
         └────┬─────┘                 └──────────┘
              │
              ▼
         ┌─────────┐
         │ SIGNED  │
         └────┬────┘
              │
              ▼
         ┌───────────┐
         │ BROADCAST │
         └─────┬─────┘
               │
     ┌─────────┴──────────┐
     ▼                    ▼
┌───────────┐        ┌────────┐
│ CONFIRMED │        │ FAILED │
└───────────┘        └────────┘
```

---

## 🧪 Running in a Sandbox Environment

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

> `asyncio` is part of the Python standard library and does not need to be installed separately. Depending on your database adapter preference, you may also need `psycopg2-binary` to wire the synchronous `WithdrawalService` class to a real PostgreSQL connection.

### 4. Set Up PostgreSQL

Start a local PostgreSQL instance and create a sandbox database:

```bash
psql -U postgres -c "CREATE DATABASE custody;"
psql -U postgres -d custody -f withdrawal.sql
```

The SQL file will:
- Create all tables and indexes
- Insert a sample ETH account with `balance = 100` and `locked_balance = 25`
- Run a complete withdrawal lifecycle end-to-end (initiation → confirmation)
- Print the final ledger state for verification

### 5. Start a Local Kafka (Docker)

```bash
docker run -d --name kafka \
  -p 9092:9092 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  apache/kafka:latest
```

### 6. Run the Outbox Publisher

The publisher has a built-in entry point that connects to PostgreSQL (`localhost/custody`) and Kafka (`localhost:9092`):

```bash
python outbox-publisher.py
```

It will poll `outbox_events` in a loop and deliver unpublished events to Kafka. Press `Ctrl+C` to stop gracefully.

### 7. Exercise the Withdrawal Service

Wire up the `WithdrawalService` with stub implementations of the policy engine and signing queue for local testing:

```python
from withdrawal import WithdrawalService

class StubPolicyEngine:
    def evaluate(self, tx):
        class Result:
            decision = "approved"
        return Result()

class StubSigningQueue:
    def send(self, **kwargs):
        print(f"[QUEUE] Enqueued: {kwargs}")

class StubDB:
    # Implement query/execute/transaction methods
    # backed by your local psycopg2 connection
    pass

service = WithdrawalService(
    db=StubDB(),
    policy_engine=StubPolicyEngine(),
    signing_queue=StubSigningQueue()
)

service.process_withdrawal(
    user_id="your-user-uuid",
    asset="ETH",
    amount=10.0,
    destination_address="0xYourSandboxAddress",
    idempotency_key="unique-key-001"
)
```

### 8. Run the Withdrawal Demo

`withdrawal.py` includes a standalone async demo that exercises the withdrawal flow (idempotency check, address validation, atomic fund lock, and transaction creation) against a live PostgreSQL database:

```bash
python withdrawal.py
```

It connects to `postgresql://postgres@localhost/custody`, creates a test account with 10 ETH if needed, and walks through each step with detailed output.

### 9. Verify the Ledger State

Run this query in psql to confirm the final state after the SQL demo:

```sql
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
```

Expected result after confirmation: `balance = 75`, `locked_balance = 0`, `available = 75`, `status = confirmed`.

---

## 📁 Project Structure

```
Crypto-Custody-Withdrawal-System-Python/
│
├── withdrawal.py          # Core WithdrawalService — full withdrawal lifecycle
├── withdrawal.sql         # PostgreSQL schema + end-to-end sandbox demo
├── outbox-publisher.py    # Async background poller — DB outbox → Kafka
└── LICENSE                # MIT License
```

---

## 🚨 Production Warning

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

## 📄 License

This project is licensed under the [MIT License](LICENSE).

---

*Built with ♥️ by [Pavon Dunbar](https://linktr.ee/pavondunbar)*
