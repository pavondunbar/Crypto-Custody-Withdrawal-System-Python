import asyncio
import json
import os
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum

import asyncpg


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
            available = balance_row.balance if balance_row else Decimal(0)
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

    def _refund_and_reject(self, tx, account_id, asset, amount):
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


async def main():
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "ledger_db")
    db_user = os.environ.get("DB_USER", "ledger_user")
    db_pass = os.environ.get("DB_PASSWORD", "")
    dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
    max_retries = 10
    retry_delay = 2
    conn = None
    for attempt in range(1, max_retries + 1):
        try:
            conn = await asyncpg.connect(dsn)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt == max_retries:
                raise
            print(
                f"[DB] Connection attempt {attempt}/{max_retries}"
                f" failed: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

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
    print("Double-Entry Withdrawal Demo")
    print("=" * 60)

    # Seed chart of accounts (idempotent)
    print("\n[Setup] Seeding chart of accounts...")
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

    # Create account (identity only — no balance columns)
    account = await conn.fetchrow(
        "SELECT id FROM accounts "
        "WHERE user_id = $1 AND asset = $2",
        user_id, asset,
    )
    if account is None:
        account_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO accounts "
            "(id, user_id, asset, created_at) "
            "VALUES ($1, $2, $3, $4)",
            account_id, user_id, asset,
            datetime.now(tz=timezone.utc),
        )
        print(f"\n[Setup] Created account {account_id}")
    else:
        account_id = account["id"]
        print(f"\n[Setup] Using existing account {account_id}")

    # Deposit 10 ETH via journal entry
    print("\n[Setup] Recording 10 ETH deposit...")
    deposit_journal_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)
    async with conn.transaction():
        await conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "($1, $2, $3, $4, $5, 0, $6, $7, $8), "
            "($1, $2, $9, $4, 0, $5, $6, $7, $8)",
            deposit_journal_id, account_id,
            "HOT_WALLET", asset, Decimal("10.0"),
            "deposit", account_id, now,
            "CUST_LIABILITY",
        )

    balance_row = await conn.fetchrow(
        "SELECT balance FROM account_balances "
        "WHERE account_id = $1",
        account_id,
    )
    print(f"  Derived balance: {balance_row['balance']}")

    # Step 1: Idempotency check
    print("\n[Step 1] Idempotency check...")
    existing = await conn.fetchrow(
        "SELECT id FROM transactions "
        "WHERE idempotency_key = $1",
        idempotency_key,
    )
    if existing:
        print(
            f"  Duplicate — returning tx {existing['id']}"
        )
        await conn.close()
        return
    print("  No duplicate found.")

    # Step 2: Address validation
    print("\n[Step 2] Address validation...")
    if not destination:
        raise InvalidAddressError("Empty address")
    print(f"  Address {destination} valid.")

    # Step 3: Atomic withdrawal initiation
    print("\n[Step 3] Atomic withdrawal initiation...")
    tx_id = uuid.uuid4()
    outbox_id = uuid.uuid4()
    journal_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)

    async with conn.transaction():
        # Pessimistic lock on account row
        locked = await conn.fetchrow(
            "SELECT id FROM accounts "
            "WHERE id = $1 FOR UPDATE",
            account_id,
        )

        # Derive balance from journal
        bal = await conn.fetchrow(
            "SELECT balance FROM account_balances "
            "WHERE account_id = $1",
            account_id,
        )
        available = bal["balance"] if bal else Decimal(0)
        print(f"  Derived balance: {available}")

        if available < amount:
            raise InsufficientBalanceError(
                f"Need {amount}, have {available}"
            )

        # INSERT immutable transaction
        await conn.execute(
            "INSERT INTO transactions "
            "(id, account_id, type, amount, "
            "destination_address, idempotency_key, "
            "created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            tx_id, account_id, "withdrawal", amount,
            destination, idempotency_key, now,
        )
        print(f"  Created transaction {tx_id}")

        # INSERT status history
        await conn.execute(
            "INSERT INTO transaction_status_history "
            "(transaction_id, status, created_at) "
            "VALUES ($1, $2, $3)",
            tx_id,
            TransactionStatus.PENDING_POLICY.value,
            now,
        )

        # INSERT journal pair:
        # DEBIT CUST_LIABILITY, CREDIT WITHDRAWAL_PENDING
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
        print(
            f"  Journal pair: DEBIT CUST_LIABILITY "
            f"{amount}, CREDIT WITHDRAWAL_PENDING {amount}"
        )

        # INSERT outbox event
        payload = json.dumps({
            "transaction_id": str(tx_id),
            "asset": asset,
            "amount": str(amount),
            "destination": destination,
        })
        await conn.execute(
            "INSERT INTO outbox_events "
            "(id, aggregate_id, event_type, "
            "payload, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            outbox_id, str(tx_id),
            "withdrawal.pending_policy", payload, now,
        )
        print(f"  Outbox event {outbox_id}")

    print("  Transaction committed.")

    # Step 4: Verify balance after initiation
    print("\n[Step 4] Post-initiation state:")
    bal = await conn.fetchrow(
        "SELECT balance FROM account_balances "
        "WHERE account_id = $1",
        account_id,
    )
    print(f"  Derived balance: {bal['balance']}")

    status = await conn.fetchrow(
        "SELECT status FROM transaction_current_status "
        "WHERE transaction_id = $1",
        tx_id,
    )
    print(f"  Transaction status: {status['status']}")

    # Step 5: Simulate on-chain confirmation
    print("\n[Step 5] Confirming withdrawal on-chain...")
    confirm_journal_id = uuid.uuid4()
    confirm_outbox_id = uuid.uuid4()
    now = datetime.now(tz=timezone.utc)

    async with conn.transaction():
        # INSERT status: confirmed
        await conn.execute(
            "INSERT INTO transaction_status_history "
            "(transaction_id, status, tx_hash, "
            "block_number, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            tx_id, TransactionStatus.CONFIRMED.value,
            "0x123456...abcdef", 12345678, now,
        )

        # INSERT journal pair:
        # DEBIT WITHDRAWAL_PENDING, CREDIT HOT_WALLET
        await conn.execute(
            "INSERT INTO journal_entries "
            "(journal_id, account_id, coa_code, asset, "
            "debit, credit, entry_type, reference_id, "
            "created_at) VALUES "
            "($1, $2, $3, $4, $5, 0, $6, $7, $8), "
            "($1, $2, $9, $4, 0, $5, $6, $7, $8)",
            confirm_journal_id, account_id,
            "WITHDRAWAL_PENDING", asset, amount,
            "withdrawal_settlement", tx_id, now,
            "HOT_WALLET",
        )
        print(
            f"  Journal pair: DEBIT WITHDRAWAL_PENDING "
            f"{amount}, CREDIT HOT_WALLET {amount}"
        )

        # INSERT outbox event
        confirm_payload = json.dumps({
            "transaction_id": str(tx_id),
            "tx_hash": "0x123456...abcdef",
            "block_number": "12345678",
            "amount": str(amount),
        })
        await conn.execute(
            "INSERT INTO outbox_events "
            "(id, aggregate_id, event_type, "
            "payload, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            confirm_outbox_id, str(tx_id),
            "withdrawal.confirmed",
            confirm_payload, now,
        )

    print("  Confirmation committed.")

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

    events = await conn.fetch(
        "SELECT event_type, published_at "
        "FROM outbox_events ORDER BY created_at"
    )
    print(f"\n  Outbox events ({len(events)}):")
    for ev in events:
        pub = ev["published_at"] or "not yet"
        print(f"    {ev['event_type']:35s} published: {pub}")

    print("\n" + "=" * 60)
    print("Done. All balances derived from journal entries.")
    print("Run outbox-publisher.py to deliver events to Kafka.")
    print("=" * 60)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
