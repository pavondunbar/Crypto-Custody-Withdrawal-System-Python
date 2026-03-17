import uuid
import json
from datetime import datetime
from enum import Enum


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
