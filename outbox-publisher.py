import asyncio
import logging

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
                self.batch_size
            )

            if not events:
                return

            for event in events:
                await self.kafka.send(
                    topic=f"custody.{event['event_type']}",
                    key=event["aggregate_id"].encode(),
                    value=event["payload"].encode()
                )

                await conn.execute(
                    "UPDATE outbox_events "
                    "SET published_at = NOW() "
                    "WHERE id = $1",
                    event["id"]
                )

    async def run_forever(self, poll_interval=1):
        while True:
            try:
                await self.poll_and_publish()
            except Exception as e:
                logger.error(f"Outbox poll failed: {e}")
            await asyncio.sleep(poll_interval)
