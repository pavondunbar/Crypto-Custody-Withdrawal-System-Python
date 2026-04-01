import asyncio
import json
import logging
import os
from datetime import datetime, timezone

import asyncpg
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

MAX_RETRIES_PER_EVENT = 5


class OutboxPublisher:
    def __init__(self, db, kafka_producer, batch_size=100):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size
        self._retry_counts = {}

    async def _move_to_dlq(self, conn, event_id, error_msg):
        """Move a permanently failed event to the DLQ."""
        await conn.execute(
            "INSERT INTO dead_letter_queue "
            "(original_event_id, error_message, "
            "retry_count, max_retries, last_retry_at) "
            "VALUES ($1, $2, $3, $4, $5)",
            event_id,
            error_msg,
            MAX_RETRIES_PER_EVENT,
            MAX_RETRIES_PER_EVENT,
            datetime.now(tz=timezone.utc),
        )
        await conn.execute(
            "UPDATE outbox_events "
            "SET published_at = NOW() "
            "WHERE id = $1",
            event_id,
        )
        self._retry_counts.pop(event_id, None)
        logger.error(
            "Event %s moved to DLQ after %d retries: %s",
            event_id, MAX_RETRIES_PER_EVENT, error_msg,
        )

    async def poll_and_publish(self):
        async with self.db.acquire() as conn:
            events = await conn.fetch(
                "SELECT id, aggregate_id, event_type, "
                "payload "
                "FROM outbox_events "
                "WHERE published_at IS NULL "
                "ORDER BY created_at "
                "LIMIT $1 "
                "FOR UPDATE SKIP LOCKED",
                self.batch_size,
            )

            if not events:
                return

            logger.info(
                "Found %d unpublished event(s)", len(events)
            )

            for event in events:
                topic = f"custody.{event['event_type']}"
                payload = event["payload"]
                if not isinstance(payload, str):
                    payload = json.dumps(payload)

                try:
                    await self.kafka.send(
                        topic=topic,
                        key=event["aggregate_id"].encode(),
                        value=payload.encode(),
                    )
                except Exception as exc:
                    eid = event["id"]
                    count = self._retry_counts.get(eid, 0) + 1
                    self._retry_counts[eid] = count
                    if count >= MAX_RETRIES_PER_EVENT:
                        await self._move_to_dlq(
                            conn, eid, str(exc)
                        )
                    else:
                        logger.warning(
                            "Kafka send failed for %s "
                            "(attempt %d/%d): %s",
                            eid, count,
                            MAX_RETRIES_PER_EVENT, exc,
                        )
                    continue

                logger.info(
                    "Published event %s -> %s",
                    event["id"], topic,
                )
                self._retry_counts.pop(event["id"], None)

                await conn.execute(
                    "UPDATE outbox_events "
                    "SET published_at = NOW() "
                    "WHERE id = $1",
                    event["id"],
                )

    async def run_forever(self, poll_interval=1):
        while True:
            try:
                await self.poll_and_publish()
            except Exception as exc:
                logger.error("Outbox poll failed: %s", exc)
            await asyncio.sleep(poll_interval)


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
    kafka_broker = os.environ.get("KAFKA_BROKER", "localhost:9092")

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
            print(
                f"[DB] Connection attempt {attempt}/{max_retries}"
                f" failed: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

    producer = AIOKafkaProducer(
        bootstrap_servers=kafka_broker,
        acks="all",
        enable_idempotence=True,
    )
    for attempt in range(1, max_retries + 1):
        try:
            await producer.start()
            break
        except Exception as exc:
            if attempt == max_retries:
                await pool.close()
                raise SystemExit(
                    f"Failed to connect to Kafka: {exc}\n"
                    "Ensure Kafka is running."
                )
            print(
                f"[Kafka] Connection attempt {attempt}/{max_retries}"
                f" failed: {exc}. Retrying in {retry_delay}s..."
            )
            await asyncio.sleep(retry_delay)

    publisher = OutboxPublisher(db=pool, kafka_producer=producer)
    print("OutboxPublisher started — polling for events…")
    print("Press Ctrl+C to stop.\n")

    try:
        await publisher.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down…")
        await producer.stop()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
