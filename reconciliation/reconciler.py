"""Reconciliation engine: replays journal entries, compares to views,
and records mismatches. Supports balance reconciliation and
deterministic state rebuild."""

import asyncio
import logging
import os
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import asyncpg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("reconciler")

VALID_TRANSITIONS = {
    None: ["pending_policy"],
    "pending_policy": ["approved", "rejected"],
    "approved": ["signed", "failed"],
    "signed": ["broadcast", "failed"],
    "broadcast": ["confirmed", "failed"],
}
TERMINAL = {"rejected", "confirmed", "failed"}


async def replay_journal_balances(conn):
    """Replay all journal entries and compute balances in memory."""
    rows = await conn.fetch(
        "SELECT account_id, asset, debit, credit "
        "FROM journal_entries "
        "WHERE coa_code = 'CUST_LIABILITY' "
        "ORDER BY created_at"
    )
    balances = defaultdict(lambda: Decimal(0))
    for row in rows:
        key = (row["account_id"], row["asset"])
        balances[key] += row["credit"] - row["debit"]
    return dict(balances)


async def get_view_balances(conn):
    """Read current balances from the account_balances view."""
    rows = await conn.fetch(
        "SELECT account_id, asset, balance "
        "FROM account_balances"
    )
    return {
        (r["account_id"], r["asset"]): r["balance"]
        for r in rows
    }


async def run_reconciliation(pool):
    """Create a run record, replay journal, compare to view,
    and record any mismatches."""
    async with pool.acquire() as conn:
        run_id = uuid.uuid4()
        await conn.execute(
            "INSERT INTO reconciliation_runs "
            "(run_id, status) VALUES ($1, 'running')",
            run_id,
        )

        replayed = await replay_journal_balances(conn)
        viewed = await get_view_balances(conn)

        all_keys = set(replayed) | set(viewed)
        mismatches = 0

        for key in all_keys:
            expected = replayed.get(key, Decimal(0))
            actual = viewed.get(key, Decimal(0))
            if expected != actual:
                mismatches += 1
                account_id, asset = key
                await conn.execute(
                    "INSERT INTO reconciliation_mismatches "
                    "(run_id, account_id, asset, "
                    "expected_balance, actual_balance, "
                    "difference) "
                    "VALUES ($1, $2, $3, $4, $5, $6)",
                    run_id, account_id, asset,
                    expected, actual, expected - actual,
                )
                logger.critical(
                    "MISMATCH account=%s asset=%s "
                    "expected=%s actual=%s",
                    account_id, asset, expected, actual,
                )

        status = "failed" if mismatches > 0 else "passed"
        await conn.execute(
            "UPDATE reconciliation_runs "
            "SET status = $1, completed_at = $2, "
            "total_accounts = $3, mismatches_found = $4 "
            "WHERE run_id = $5",
            status,
            datetime.now(tz=timezone.utc),
            len(all_keys),
            mismatches,
            run_id,
        )
        logger.info(
            "Reconciliation run=%s status=%s "
            "accounts=%d mismatches=%d",
            run_id, status, len(all_keys), mismatches,
        )
        return status


async def run_state_rebuild(pool):
    """Replay status history and verify transitions are valid.
    Also runs balance reconciliation."""
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT transaction_id, status, created_at "
            "FROM transaction_status_history "
            "ORDER BY transaction_id, created_at"
        )

        current = {}
        errors = []
        for row in rows:
            tx_id = row["transaction_id"]
            prev = current.get(tx_id)
            new = row["status"]
            allowed = VALID_TRANSITIONS.get(prev, [])
            if new not in allowed:
                errors.append(
                    f"tx={tx_id}: {prev} -> {new} invalid"
                )
            current[tx_id] = new

        if errors:
            for err in errors:
                logger.critical("STATE REBUILD: %s", err)
            logger.critical(
                "State rebuild found %d invalid transitions",
                len(errors),
            )
        else:
            logger.info(
                "State rebuild: all %d transactions valid",
                len(current),
            )

    balance_status = await run_reconciliation(pool)
    return "passed" if not errors and balance_status == "passed" else "failed"


async def main():
    db_host = os.environ.get("DB_HOST", "localhost")
    db_port = os.environ.get("DB_PORT", "5432")
    db_name = os.environ.get("DB_NAME", "ledger_db")
    db_user = os.environ.get("DB_USER", "readonly_user")
    db_pass = os.environ.get("DB_PASSWORD", "")
    dsn = (
        f"postgresql://{db_user}:{db_pass}"
        f"@{db_host}:{db_port}/{db_name}"
    )
    interval = int(os.environ.get("RECON_INTERVAL", "60"))
    mode = os.environ.get("RECON_MODE", "reconcile")

    max_retries = 10
    retry_delay = 2
    pool = None
    for attempt in range(1, max_retries + 1):
        try:
            pool = await asyncpg.create_pool(dsn)
            break
        except (OSError, asyncpg.PostgresError) as exc:
            if attempt == max_retries:
                raise
            logger.warning(
                "DB attempt %d/%d failed: %s. Retrying...",
                attempt, max_retries, exc,
            )
            await asyncio.sleep(retry_delay)

    logger.info(
        "Reconciler started (mode=%s, interval=%ds)",
        mode, interval,
    )

    try:
        while True:
            try:
                if mode == "rebuild":
                    await run_state_rebuild(pool)
                else:
                    await run_reconciliation(pool)
            except Exception:
                logger.exception("Reconciliation cycle failed")
            await asyncio.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
