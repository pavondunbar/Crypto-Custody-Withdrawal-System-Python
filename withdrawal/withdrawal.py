import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import asyncpg

from rbac import (
    check_permission,
    insert_audit_event,
    validate_transition,
)


class InvalidAddressError(Exception):
    """Raised when a destination address fails validation."""


class InsufficientBalanceError(Exception):
    """Raised when available balance is too low."""


class TransactionStatus(Enum):
    PENDING_POLICY = "pending_policy"
    APPROVED = "approved"
    REJECTED = "rejected"
    SIGNED = "signed"
    BROADCAST = "broadcast"
    CONFIRMED = "confirmed"
    FAILED = "failed"


class WithdrawalService:
    def __init__(self, db, policy_engine, signing_queue):
        self.db = db
        self.policy_engine = policy_engine
        self.signing_queue = signing_queue

    def process_withdrawal(
        self, user_id, asset, amount,
        destination_address, idempotency_key
    ):
        existing = self.db.query(
            "SELECT t.*, tcs.status "
            "FROM transactions t "
            "JOIN transaction_current_status tcs "
            "  ON tcs.transaction_id = t.id "
            "WHERE t.idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        if not self._validate_address(asset, destination_address):
            raise InvalidAddressError()

        with self.db.transaction() as conn:
            account = conn.query(
                "SELECT id FROM accounts "
                "WHERE user_id = %s AND asset = %s "
                "FOR UPDATE",
                (user_id, asset)
            )

            balance_row = conn.query(
                "SELECT balance FROM account_balances "
                "WHERE account_id = %s",
                (account.id,)
            )
            available = (
                balance_row.balance if balance_row
                else Decimal(0)
            )
            if available < amount:
                raise InsufficientBalanceError()

            tx_id = uuid.uuid4()
            now = datetime.now(tz=timezone.utc)

            conn.execute(
                "INSERT INTO transactions "
                "(id, account_id, type, amount, "
                "destination_address, idempotency_key, "
                "created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (tx_id, account.id, "withdrawal", amount,
                 destination_address, idempotency_key, now)
            )

            conn.execute(
                "INSERT INTO transaction_status_history "
                "(transaction_id, status, created_at) "
                "VALUES (%s, %s, %s)",
                (tx_id,
                 TransactionStatus.PENDING_POLICY.value,
                 now)
            )

            self._insert_journal_pair(
                conn, account.id, asset, amount,
                debit_coa="CUST_LIABILITY",
                credit_coa="WITHDRAWAL_PENDING",
                entry_type="withdrawal_initiation",
                reference_id=tx_id,
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, "
                "payload, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (uuid.uuid4(), str(tx_id),
                 "withdrawal.pending_policy",
                 json.dumps({
                     "transaction_id": str(tx_id),
                     "asset": asset,
                     "amount": str(amount),
                     "destination": destination_address,
                 }),
                 now)
            )

            tx = conn.query(
                "SELECT * FROM transactions WHERE id = %s",
                (tx_id,)
            )

        result = self.policy_engine.evaluate(tx)

        if result.decision == "rejected":
            self._refund_and_reject(
                tx, account.id, asset, amount
            )
            return tx

        self.signing_queue.send(
            message_body={
                "transaction_id": str(tx_id),
                "asset": asset,
                "amount": str(amount),
                "destination": destination_address,
            },
            message_group_id=str(account.id),
            message_deduplication_id=str(tx_id),
        )

        return tx

    def _refund_and_reject(
        self, tx, account_id, asset, amount
    ):
        with self.db.transaction() as conn:
            self._insert_journal_pair(
                conn, account_id, asset, amount,
                debit_coa="WITHDRAWAL_PENDING",
                credit_coa="CUST_LIABILITY",
                entry_type="withdrawal_reversal",
                reference_id=tx.id,
            )

            conn.execute(
                "INSERT INTO transaction_status_history "
                "(transaction_id, status, created_at) "
                "VALUES (%s, %s, %s)",
                (tx.id, TransactionStatus.REJECTED.value,
                 datetime.now(tz=timezone.utc))
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, "
                "payload, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (uuid.uuid4(), str(tx.id),
                 "withdrawal.rejected",
                 json.dumps({
                     "transaction_id": str(tx.id),
                     "reason": "policy_rejected",
                 }),
                 datetime.now(tz=timezone.utc))
            )

    def confirm_withdrawal(
        self, tx_id, account_id, asset, amount,
        tx_hash, block_number
    ):
        with self.db.transaction() as conn:
            self._insert_journal_pair(
                conn, account_id, asset, amount,
                debit_coa="WITHDRAWAL_PENDING",
                credit_coa="HOT_WALLET",
                entry_type="withdrawal_settlement",
                reference_id=tx_id,
            )

            conn.execute(
                "INSERT INTO transaction_status_history "
                "(transaction_id, status, tx_hash, "
                "block_number, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (tx_id, TransactionStatus.CONFIRMED.value,
                 tx_hash, block_number,
                 datetime.now(tz=timezone.utc))
            )

            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, "
                "payload, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (uuid.uuid4(), str(tx_id),
                 "withdrawal.confirmed",
                 json.dumps({
                     "transaction_id": str(tx_id),
                     "tx_hash": tx_hash,
                     "block_number": str(block_number),
                     "amount": str(amount),
                 }),
                 datetime.now(tz=timezone.utc))
            )

    def _insert_journal_pair(
        self, conn, account_id, asset, amount,
        debit_coa, credit_coa, entry_type, reference_id
    ):
        journal_id = uuid.uuid4()
        now = datetime.now(tz=timezone.utc)
        conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "(%s, %s, %s, %s, %s, 0, %s, %s, %s), "
            "(%s, %s, %s, %s, 0, %s, %s, %s, %s)",
            (journal_id, account_id, debit_coa, asset,
             amount, entry_type, reference_id, now,
             journal_id, account_id, credit_coa, asset,
             amount, entry_type, reference_id, now)
        )

    def _validate_address(self, asset, address):
        if not address:
            return False
        return True


# --------------------------------------------------
# Demo helper functions
# --------------------------------------------------

async def connect_db():
    """Connect to Postgres with retry logic."""
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "ledger_db")
    db_user = os.environ.get("DB_USER", "ledger_user")
    db_pass = os.environ.get("DB_PASSWORD", "")
    dsn = (
        f"postgresql://{db_user}:{db_pass}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    max_retries = 10
    for attempt in range(1, max_retries + 1):
        try:
            return await asyncpg.connect(dsn)
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt == max_retries:
                raise
            print(
                f"[DB] Attempt {attempt}/{max_retries} "
                f"failed: {exc}. Retrying..."
            )
            await asyncio.sleep(2)


async def seed_chart_of_accounts(conn):
    """Idempotent seed of the chart of accounts."""
    for code, name, acct_type, normal in [
        ("CUST_LIABILITY", "Customer Liability",
         "liability", "credit"),
        ("HOT_WALLET", "Hot Wallet",
         "asset", "debit"),
        ("WITHDRAWAL_PENDING", "Withdrawal Pending",
         "liability", "credit"),
        ("FEE_REVENUE", "Fee Revenue",
         "revenue", "credit"),
    ]:
        await conn.execute(
            "INSERT INTO chart_of_accounts "
            "(code, name, account_type, normal_balance) "
            "VALUES ($1, $2, $3, $4) "
            "ON CONFLICT (code) DO NOTHING",
            code, name, acct_type, normal,
        )
    print("  Chart of accounts ready.")


async def create_or_get_account(conn, user_id, asset):
    """Return existing account_id or create a new one."""
    row = await conn.fetchrow(
        "SELECT id FROM accounts "
        "WHERE user_id = $1 AND asset = $2",
        user_id, asset,
    )
    if row:
        print(f"  Using existing account {row['id']}")
        return row["id"]
    account_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO accounts "
        "(id, user_id, asset, created_at) "
        "VALUES ($1, $2, $3, $4)",
        account_id, user_id, asset,
        datetime.now(tz=timezone.utc),
    )
    print(f"  Created account {account_id}")
    return account_id


async def record_deposit(conn, account_id, asset, amount):
    """Record a deposit via a balanced journal pair."""
    journal_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "($1, $2, $3, $4, $5, 0, $6, $7, $8), "
            "($1, $2, $9, $4, 0, $5, $6, $7, $8)",
            journal_id, account_id,
            "HOT_WALLET", asset, amount,
            "deposit", account_id, now,
            "CUST_LIABILITY",
        )
    bal = await conn.fetchrow(
        "SELECT balance FROM account_balances "
        "WHERE account_id = $1",
        account_id,
    )
    print(f"  Deposited {amount} {asset}. "
          f"Balance: {bal['balance']}")


async def initiate_withdrawal(
    conn, account_id, asset, amount,
    destination, idempotency_key,
    actor_role, actor_id, trace_id,
):
    """Atomically initiate a withdrawal with audit event."""
    tx_id = uuid.uuid4()
    outbox_id = uuid.uuid4()
    journal_id = uuid.uuid4()
    request_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)

    async with conn.transaction():
        await conn.fetchrow(
            "SELECT id FROM accounts "
            "WHERE id = $1 FOR UPDATE",
            account_id,
        )
        bal = await conn.fetchrow(
            "SELECT balance FROM account_balances "
            "WHERE account_id = $1",
            account_id,
        )
        available = bal["balance"] if bal else Decimal(0)
        if available < amount:
            raise InsufficientBalanceError(
                f"Need {amount}, have {available}"
            )

        await conn.execute(
            "INSERT INTO transactions "
            "(id, account_id, type, amount, "
            "destination_address, idempotency_key, "
            "created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            tx_id, account_id, "withdrawal", amount,
            destination, idempotency_key, now,
        )
        await conn.execute(
            "INSERT INTO transaction_status_history "
            "(transaction_id, status, metadata, "
            "created_at) "
            "VALUES ($1, $2, $3, $4)",
            tx_id,
            TransactionStatus.PENDING_POLICY.value,
            json.dumps({
                "actor_role": actor_role,
                "actor_id": actor_id,
                "trace_id": str(trace_id),
            }),
            now,
        )
        await conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "($1, $2, $3, $4, $5, 0, $6, $7, $8), "
            "($1, $2, $9, $4, 0, $5, $6, $7, $8)",
            journal_id, account_id,
            "CUST_LIABILITY", asset, amount,
            "withdrawal_initiation", tx_id, now,
            "WITHDRAWAL_PENDING",
        )
        payload = json.dumps({
            "transaction_id": str(tx_id),
            "asset": asset,
            "amount": str(amount),
            "destination": destination,
            "trace_id": str(trace_id),
        })
        await conn.execute(
            "INSERT INTO outbox_events "
            "(id, aggregate_id, event_type, "
            "payload, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            outbox_id, str(tx_id),
            "withdrawal.pending_policy", payload, now,
        )
        await insert_audit_event(
            conn, actor_role, actor_id,
            "withdrawal.initiated",
            "transaction", tx_id,
            request_id=request_id,
            trace_id=trace_id,
            metadata=json.dumps({
                "amount": str(amount), "asset": asset,
            }),
        )

    print(f"  Created tx {tx_id}, balance now {available - amount}")
    return tx_id


async def transition_status(
    conn, tx_id, new_status,
    actor_role, actor_id, trace_id,
    tx_hash=None, block_number=None,
):
    """Transition status with audit event and outbox."""
    current = await conn.fetchrow(
        "SELECT status FROM transaction_current_status "
        "WHERE transaction_id = $1",
        tx_id,
    )
    current_val = current["status"] if current else None
    validate_transition(current_val, new_status)

    now = datetime.now(tz=timezone.utc)
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO transaction_status_history "
            "(transaction_id, status, tx_hash, "
            "block_number, metadata, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6)",
            tx_id, new_status,
            tx_hash, block_number,
            json.dumps({
                "actor_role": actor_role,
                "actor_id": actor_id,
                "trace_id": str(trace_id),
            }),
            now,
        )
        payload = {
            "transaction_id": str(tx_id),
            "status": new_status,
            "trace_id": str(trace_id),
        }
        if tx_hash:
            payload["tx_hash"] = tx_hash
        if block_number is not None:
            payload["block_number"] = str(block_number)
        await conn.execute(
            "INSERT INTO outbox_events "
            "(id, aggregate_id, event_type, "
            "payload, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            uuid.uuid4(), str(tx_id),
            f"withdrawal.{new_status}",
            json.dumps(payload), now,
        )
        await insert_audit_event(
            conn, actor_role, actor_id,
            f"withdrawal.{new_status}",
            "transaction", tx_id,
            trace_id=trace_id,
            metadata=json.dumps(payload),
        )
    print(f"  {current_val} -> {new_status}")


async def confirm_on_chain(
    conn, tx_id, account_id, asset, amount,
    tx_hash, block_number, trace_id,
):
    """Settlement journal pair + confirmed status."""
    journal_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "($1, $2, $3, $4, $5, 0, $6, $7, $8), "
            "($1, $2, $9, $4, 0, $5, $6, $7, $8)",
            journal_id, account_id,
            "WITHDRAWAL_PENDING", asset, amount,
            "withdrawal_settlement", tx_id, now,
            "HOT_WALLET",
        )
    await transition_status(
        conn, tx_id, TransactionStatus.CONFIRMED.value,
        "system", "on-chain-tracker", trace_id,
        tx_hash=tx_hash, block_number=block_number,
    )


async def run_inline_reconciliation(conn, account_id):
    """Replay journal for one account, compare to view."""
    rows = await conn.fetch(
        "SELECT debit, credit FROM journal_entries "
        "WHERE account_id = $1 AND coa_code = 'CUST_LIABILITY'",
        account_id,
    )
    replayed = sum(
        r["credit"] - r["debit"] for r in rows
    )
    view_row = await conn.fetchrow(
        "SELECT balance FROM account_balances "
        "WHERE account_id = $1",
        account_id,
    )
    view_bal = view_row["balance"] if view_row else Decimal(0)
    match = "PASS" if replayed == view_bal else "FAIL"
    print(f"  Replayed={replayed}  View={view_bal}  [{match}]")
    return replayed == view_bal


async def print_audit_trail(conn, trace_id):
    """Display all audit events for a trace."""
    rows = await conn.fetch(
        "SELECT action, actor_role, actor_id, "
        "resource_id, created_at "
        "FROM audit_events WHERE trace_id = $1 "
        "ORDER BY created_at",
        trace_id,
    )
    print(f"  Audit trail ({len(rows)} events):")
    for r in rows:
        print(
            f"    {r['action']:40s} "
            f"by {r['actor_role']}/{r['actor_id']}  "
            f"resource={r['resource_id']}"
        )


async def main():
    conn = await connect_db()

    trace_id = uuid.uuid4()
    user_id = uuid.UUID(
        "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    )
    asset = "ETH"
    amount = Decimal("1.5")
    destination = (
        "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18"
    )
    idempotency_key = str(uuid.uuid4())

    print("=" * 60)
    print("Double-Entry Withdrawal Demo (Full State Machine)")
    print("=" * 60)

    # Setup
    print("\n[Setup] Seeding chart of accounts...")
    await seed_chart_of_accounts(conn)

    print("\n[Setup] Account...")
    account_id = await create_or_get_account(
        conn, user_id, asset
    )

    print("\n[Setup] Deposit 10 ETH...")
    await record_deposit(
        conn, account_id, asset, Decimal("10.0")
    )

    # RBAC check
    print("\n[RBAC] Checking admin permission...")
    check_permission("admin", "approve_withdrawal")
    print("  admin has approve_withdrawal: OK")

    roles = await conn.fetch("SELECT code, permissions FROM roles")
    print(f"  Roles in DB: {len(roles)}")
    for r in roles:
        print(f"    {r['code']}: {r['permissions']}")

    # Step 1: Initiate (PENDING_POLICY)
    print("\n[Step 1] Initiate withdrawal...")
    tx_id = await initiate_withdrawal(
        conn, account_id, asset, amount,
        destination, idempotency_key,
        actor_role="admin",
        actor_id="user-alice",
        trace_id=trace_id,
    )

    # Step 2: Approve (PENDING_POLICY -> APPROVED)
    print("\n[Step 2] Approve withdrawal...")
    await transition_status(
        conn, tx_id, TransactionStatus.APPROVED.value,
        "admin", "policy-engine", trace_id,
    )

    # Step 3: Sign (APPROVED -> SIGNED)
    print("\n[Step 3] Sign withdrawal...")
    await transition_status(
        conn, tx_id, TransactionStatus.SIGNED.value,
        "signer", "mpc-cluster", trace_id,
    )

    # Step 4: Broadcast (SIGNED -> BROADCAST)
    print("\n[Step 4] Broadcast withdrawal...")
    await transition_status(
        conn, tx_id, TransactionStatus.BROADCAST.value,
        "system", "broadcaster", trace_id,
    )

    # Step 5: Confirm on-chain (BROADCAST -> CONFIRMED)
    print("\n[Step 5] Confirm on-chain...")
    await confirm_on_chain(
        conn, tx_id, account_id, asset, amount,
        "0xabc123...def456", 19_500_000, trace_id,
    )

    # Step 6: Final state
    print("\n[Step 6] Final state:")
    bal = await conn.fetchrow(
        "SELECT balance FROM account_balances "
        "WHERE account_id = $1",
        account_id,
    )
    print(f"  Derived balance: {bal['balance']}")

    status = await conn.fetchrow(
        "SELECT status, tx_hash, block_number "
        "FROM transaction_current_status "
        "WHERE transaction_id = $1",
        tx_id,
    )
    print(f"  Status:       {status['status']}")
    print(f"  tx_hash:      {status['tx_hash']}")
    print(f"  block_number: {status['block_number']}")

    # Journal entries
    entries = await conn.fetch(
        "SELECT coa_code, debit, credit, entry_type "
        "FROM journal_entries "
        "WHERE account_id = $1 "
        "ORDER BY created_at, coa_code",
        account_id,
    )
    print(f"\n  Journal entries ({len(entries)} total):")
    for e in entries:
        side = (
            f"DR {e['debit']}"
            if e["debit"] > 0
            else f"CR {e['credit']}"
        )
        print(
            f"    {e['entry_type']:30s} "
            f"{e['coa_code']:25s} {side}"
        )

    # Outbox events
    events = await conn.fetch(
        "SELECT event_type, published_at "
        "FROM outbox_events ORDER BY created_at"
    )
    print(f"\n  Outbox events ({len(events)}):")
    for ev in events:
        pub = ev["published_at"] or "not yet"
        print(
            f"    {ev['event_type']:35s} "
            f"published: {pub}"
        )

    # Inline reconciliation
    print("\n[Step 7] Inline reconciliation:")
    await run_inline_reconciliation(conn, account_id)

    # Audit trail
    print(f"\n[Step 8] Audit trail (trace={trace_id}):")
    await print_audit_trail(conn, trace_id)

    print("\n" + "=" * 60)
    print("Done. Full state machine exercised:")
    print("  PENDING_POLICY -> APPROVED -> SIGNED "
          "-> BROADCAST -> CONFIRMED")
    print("All balances derived from journal entries.")
    print("=" * 60)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
