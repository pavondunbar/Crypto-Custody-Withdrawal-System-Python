import asyncio
import json
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum

import asyncpg


class InvalidAddressError(Exception):
    """Raised when a destination address fails validation."""


class InsufficientBalanceError(Exception):
    """Raised when available balance is too low for the withdrawal."""


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
        # ---- STEP 1: Idempotency check ----
        # Must be FIRST — before any DB writes
        existing = self.db.query(
            "SELECT * FROM transactions "
            "WHERE idempotency_key = %s",
            (idempotency_key,)
        )
        if existing:
            return existing

        # ---- STEP 2: Validate destination address ----
        if not self._validate_address(asset, destination_address):
            raise InvalidAddressError()

        # ---- STEP 3: Lock funds + create transaction ----
        # Single atomic DB transaction
        with self.db.transaction() as conn:

            # Pessimistic lock — prevents double spend
            account = conn.query(
                "SELECT id, balance, locked_balance "
                "FROM accounts "
                "WHERE user_id = %s AND asset = %s "
                "FOR UPDATE",
                (user_id, asset)
            )

            # Available = balance - locked_balance
            available = account.balance - account.locked_balance
            if available < amount:
                raise InsufficientBalanceError()

            # Lock the funds
            conn.execute(
                "UPDATE accounts "
                "SET locked_balance = locked_balance + %s, "
                "updated_at = NOW() "
                "WHERE id = %s",
                (amount, account.id)
            )

            # Create transaction record
            tx_id = uuid.uuid4()
            tx = conn.execute(
                "INSERT INTO transactions "
                "(id, account_id, type, amount, status, "
                "destination_address, idempotency_key, created_at) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) "
                "RETURNING *",
                (tx_id, account.id, "withdrawal", amount,
                 TransactionStatus.PENDING_POLICY.value,
                 destination_address, idempotency_key,
                 datetime.utcnow())
            )

            # Write to outbox IN SAME TRANSACTION
            # If Kafka is down, event is safe until poller picks it up
            conn.execute(
                "INSERT INTO outbox_events "
                "(id, aggregate_id, event_type, payload, created_at) "
                "VALUES (%s, %s, %s, %s, %s)",
                (uuid.uuid4(), str(tx_id),
                 "withdrawal.pending_policy",
                 json.dumps({
                     "transaction_id": str(tx_id),
                     "asset": asset,
                     "amount": str(amount),
                     "destination": destination_address
                 }),
                 datetime.utcnow())
            )

        # ---- STEP 4: Policy evaluation ----
        # OUTSIDE the DB transaction
        # Never hold a row lock during external calls
        result = self.policy_engine.evaluate(tx)

        if result.decision == "rejected":
            self._refund_and_reject(tx, account.id, amount)
            return tx

        # ---- STEP 5: Publish to signing queue ----
        # account.id as group ID = per-account FIFO ordering
        self.signing_queue.send(
            message_body={
                "transaction_id": str(tx_id),
                "asset": asset,
                "amount": str(amount),
                "destination": destination_address
            },
            message_group_id=str(account.id),
            message_deduplication_id=str(tx_id)
        )

        return tx

    def _refund_and_reject(self, tx, account_id, amount):
        # Release the lock — funds were never sent
        self.db.execute(
            "UPDATE accounts "
            "SET locked_balance = locked_balance - %s, "
            "updated_at = NOW() "
            "WHERE id = %s",
            (amount, account_id)
        )

        self.db.execute(
            "UPDATE transactions "
            "SET status = %s "
            "WHERE id = %s",
            (TransactionStatus.REJECTED.value, tx.id)
        )

    def _validate_address(self, asset, address):
        # Basic validation — non-empty, correct format for asset
        if not address:
            return False
        return True


async def main():
    dsn = "postgresql://postgres@localhost/custody"
    conn = await asyncpg.connect(dsn)

    user_id = uuid.UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
    asset = "ETH"
    amount = Decimal("1.5")
    destination = "0x742d35Cc6634C0532925a3b844Bc9e7595f2bD18"
    idempotency_key = str(uuid.uuid4())

    print("=" * 60)
    print("Withdrawal Demo — step-by-step flow")
    print("=" * 60)

    # Ensure a test account exists
    account = await conn.fetchrow(
        "SELECT id, balance, locked_balance "
        "FROM accounts "
        "WHERE user_id = $1 AND asset = $2",
        user_id,
        asset,
    )
    if account is None:
        account_id = uuid.uuid4()
        now = datetime.utcnow()
        await conn.execute(
            "INSERT INTO accounts "
            "(id, user_id, asset, balance, locked_balance, "
            "created_at, updated_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7)",
            account_id,
            user_id,
            asset,
            Decimal("10.0"),
            Decimal("0.0"),
            now,
            now,
        )
        account = await conn.fetchrow(
            "SELECT id, balance, locked_balance "
            "FROM accounts WHERE id = $1",
            account_id,
        )
        print(f"\nCreated test account {account_id}")
    else:
        account_id = account["id"]
        print(f"\nUsing existing account {account_id}")

    print(
        f"  balance={account['balance']}, "
        f"locked={account['locked_balance']}, "
        f"available="
        f"{account['balance'] - account['locked_balance']}"
    )

    # Step 1: Idempotency check
    print("\n[Step 1] Idempotency check…")
    existing = await conn.fetchrow(
        "SELECT id FROM transactions "
        "WHERE idempotency_key = $1",
        idempotency_key,
    )
    if existing:
        print(f"  Duplicate request — returning existing tx {existing['id']}")
        await conn.close()
        return
    print("  No duplicate found — proceeding.")

    # Step 2: Validate address
    print("\n[Step 2] Address validation…")
    if not destination:
        raise InvalidAddressError("Destination address is empty")
    print(f"  Address {destination} is valid.")

    # Step 3: Lock funds + create transaction (single DB transaction)
    print("\n[Step 3] Atomic lock-funds + create transaction…")
    tx_id = uuid.uuid4()
    outbox_id = uuid.uuid4()
    now = datetime.utcnow()

    async with conn.transaction():
        # Pessimistic lock
        locked = await conn.fetchrow(
            "SELECT id, balance, locked_balance "
            "FROM accounts "
            "WHERE id = $1 "
            "FOR UPDATE",
            account_id,
        )
        available = locked["balance"] - locked["locked_balance"]
        print(f"  Locked account — available: {available}")

        if available < amount:
            raise InsufficientBalanceError(
                f"Need {amount}, have {available}"
            )

        # Lock funds
        await conn.execute(
            "UPDATE accounts "
            "SET locked_balance = locked_balance + $1, "
            "updated_at = NOW() "
            "WHERE id = $2",
            amount,
            account_id,
        )
        print(f"  Locked {amount} {asset}")

        # Insert transaction
        await conn.execute(
            "INSERT INTO transactions "
            "(id, account_id, type, amount, status, "
            "destination_address, idempotency_key, created_at) "
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
            tx_id,
            account_id,
            "withdrawal",
            amount,
            TransactionStatus.PENDING_POLICY.value,
            destination,
            idempotency_key,
            now,
        )
        print(f"  Created transaction {tx_id}")

        # Insert outbox event (same transaction)
        payload = json.dumps({
            "transaction_id": str(tx_id),
            "asset": asset,
            "amount": str(amount),
            "destination": destination,
        })
        await conn.execute(
            "INSERT INTO outbox_events "
            "(id, aggregate_id, event_type, payload, created_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            outbox_id,
            str(tx_id),
            "withdrawal.pending_policy",
            payload,
            now,
        )
        print(f"  Created outbox event {outbox_id}")

    print("\n  Transaction committed.")

    # Step 4: Verify final state
    print("\n[Step 4] Final account state:")
    final = await conn.fetchrow(
        "SELECT balance, locked_balance "
        "FROM accounts WHERE id = $1",
        account_id,
    )
    print(f"  balance       = {final['balance']}")
    print(f"  locked_balance = {final['locked_balance']}")
    print(
        f"  available     = "
        f"{final['balance'] - final['locked_balance']}"
    )

    tx_row = await conn.fetchrow(
        "SELECT id, status, destination_address "
        "FROM transactions WHERE id = $1",
        tx_id,
    )
    print(f"\n  Transaction: {tx_row['id']}")
    print(f"  Status:      {tx_row['status']}")
    print(f"  Destination: {tx_row['destination_address']}")

    outbox_row = await conn.fetchrow(
        "SELECT id, event_type, published_at "
        "FROM outbox_events WHERE id = $1",
        outbox_id,
    )
    print(f"\n  Outbox event: {outbox_row['id']}")
    print(f"  Event type:   {outbox_row['event_type']}")
    print(f"  Published:    {outbox_row['published_at'] or 'not yet'}")

    print("\n" + "=" * 60)
    print("Done. Run outbox-publisher.py to publish the event to Kafka.")
    print("=" * 60)

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
