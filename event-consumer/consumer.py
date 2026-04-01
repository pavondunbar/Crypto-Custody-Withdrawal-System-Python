"""Kafka event consumer: subscribes to custody.withdrawal.*
topics and routes events to handlers with audit logging."""

import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone

import aiohttp
import asyncpg
from aiokafka import AIOKafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("event-consumer")


async def insert_audit_event(
    conn, action, resource_id, metadata_dict,
):
    """Append an audit row for a consumed event."""
    await conn.execute(
        "INSERT INTO audit_events "
        "(trace_id, actor_role, actor_id, action, "
        "resource_type, resource_id, metadata, "
        "created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
        uuid.uuid4(),
        "system",
        "event-consumer",
        action,
        "transaction",
        resource_id,
        json.dumps(metadata_dict),
        datetime.now(tz=timezone.utc),
    )


async def handle_pending_policy(conn, payload):
    """Log audit event for new withdrawal."""
    tx_id = uuid.UUID(payload["transaction_id"])
    await insert_audit_event(
        conn,
        "withdrawal.pending_policy.consumed",
        tx_id,
        {"asset": payload.get("asset"),
         "amount": payload.get("amount")},
    )
    logger.info("Processed pending_policy for tx=%s", tx_id)


async def handle_approved(conn, payload, gateway_url):
    """Log audit event and forward to signing gateway."""
    tx_id = uuid.UUID(payload["transaction_id"])
    await insert_audit_event(
        conn,
        "withdrawal.approved.consumed",
        tx_id,
        {"asset": payload.get("asset"),
         "amount": payload.get("amount")},
    )

    sign_payload = {
        "transaction_id": payload["transaction_id"],
        "asset": payload.get("asset", ""),
        "amount": payload.get("amount", "0"),
        "destination": payload.get("destination", ""),
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{gateway_url}/sign",
                json=sign_payload,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                body = await resp.json()
                logger.info(
                    "Signing gateway responded %d for tx=%s",
                    resp.status, tx_id,
                )
    except Exception:
        logger.exception(
            "Failed to call signing gateway for tx=%s", tx_id
        )


async def handle_signed(conn, payload):
    """Log audit event for signed transaction."""
    tx_id = uuid.UUID(payload["transaction_id"])
    await insert_audit_event(
        conn,
        "withdrawal.signed.consumed",
        tx_id,
        payload,
    )
    logger.info("Processed signed for tx=%s", tx_id)


async def handle_confirmed(conn, payload):
    """Log final audit event for confirmed transaction."""
    tx_id = uuid.UUID(payload["transaction_id"])
    await insert_audit_event(
        conn,
        "withdrawal.confirmed.consumed",
        tx_id,
        {"tx_hash": payload.get("tx_hash"),
         "block_number": payload.get("block_number")},
    )
    logger.info("Processed confirmed for tx=%s", tx_id)


async def handle_rejected(conn, payload):
    """Log audit event for rejected transaction."""
    tx_id = uuid.UUID(payload["transaction_id"])
    await insert_audit_event(
        conn,
        "withdrawal.rejected.consumed",
        tx_id,
        {"reason": payload.get("reason")},
    )
    logger.info("Processed rejected for tx=%s", tx_id)


TOPIC_HANDLERS = {
    "custody.withdrawal.pending_policy": "pending_policy",
    "custody.withdrawal.approved": "approved",
    "custody.withdrawal.signed": "signed",
    "custody.withdrawal.confirmed": "confirmed",
    "custody.withdrawal.rejected": "rejected",
}


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
    kafka_broker = os.environ.get(
        "KAFKA_BROKER", "localhost:9092"
    )
    gateway_url = os.environ.get(
        "GATEWAY_URL", "http://signing-gateway:8000"
    )

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
                "DB attempt %d/%d failed: %s",
                attempt, max_retries, exc,
            )
            await asyncio.sleep(retry_delay)

    consumer = AIOKafkaConsumer(
        *TOPIC_HANDLERS.keys(),
        bootstrap_servers=kafka_broker,
        group_id="custody-event-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
    )
    for attempt in range(1, max_retries + 1):
        try:
            await consumer.start()
            break
        except Exception as exc:
            if attempt == max_retries:
                await pool.close()
                raise SystemExit(
                    f"Failed to connect to Kafka: {exc}"
                )
            logger.warning(
                "Kafka attempt %d/%d failed: %s",
                attempt, max_retries, exc,
            )
            await asyncio.sleep(retry_delay)

    logger.info("Event consumer started, subscribed to %d topics",
                len(TOPIC_HANDLERS))

    try:
        async for msg in consumer:
            try:
                payload = json.loads(msg.value.decode())
            except (json.JSONDecodeError, UnicodeDecodeError):
                logger.error(
                    "Bad message on %s offset=%d",
                    msg.topic, msg.offset,
                )
                continue

            handler_key = TOPIC_HANDLERS.get(msg.topic)
            if not handler_key:
                continue

            async with pool.acquire() as conn:
                if handler_key == "pending_policy":
                    await handle_pending_policy(conn, payload)
                elif handler_key == "approved":
                    await handle_approved(
                        conn, payload, gateway_url
                    )
                elif handler_key == "signed":
                    await handle_signed(conn, payload)
                elif handler_key == "confirmed":
                    await handle_confirmed(conn, payload)
                elif handler_key == "rejected":
                    await handle_rejected(conn, payload)
    except KeyboardInterrupt:
        pass
    finally:
        await consumer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
