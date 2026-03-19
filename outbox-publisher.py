import asyncio
import logging

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
    pool = await asyncpg.create_pool(
        "postgresql://postgres@localhost/custody",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
    )
    try:
        await producer.start()
    except Exception as e:
        await pool.close()
        raise SystemExit(
            f"Failed to connect to Kafka: {e}\n"
            "Ensure Kafka is running on localhost:9092."
        )

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
