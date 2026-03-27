import asyncio
import logging
import os

import asyncpg
from aiokafka import AIOKafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

class OutboxPublisher:
    def __init__(self, db, kafka_producer, batch_size=100):
        self.db = db
        self.kafka = kafka_producer
        self.batch_size = batch_size

    async def poll_and_publish(self):
        async with self.db.acquire() as conn:
            events = await conn.fetch(
                "SELECT id, aggregate_id, event_type, payload "
                "FROM outbox_events "
                "WHERE published_at IS NULL "
                "ORDER BY created_at "
                "LIMIT $1 "
                "FOR UPDATE SKIP LOCKED",
                self.batch_size,
            )

            if not events:
                return

            logger.info("Found %d unpublished event(s)", len(events))

            for event in events:
                topic = f"custody.{event['event_type']}"
                payload = event["payload"]
                if not isinstance(payload, str):
                    import json
                    payload = json.dumps(payload)

                await self.kafka.send(
                    topic=topic,
                    key=event["aggregate_id"].encode(),
                    value=payload.encode(),
                )
                logger.info(
                    "Published event %s -> %s",
                    event["id"],
                    topic,
                )

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
            except Exception as e:
                logger.error("Outbox poll failed: %s", e)
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
