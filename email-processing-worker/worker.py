import asyncio
import json
import os

import redis.asyncio as redis
from dotenv import load_dotenv

from common.utils import get_logger, Queues

load_dotenv()


r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379))
)

logger = get_logger()


async def worker():
    while True:
        message = await r.brpop([Queues.EMAIL_INGESTION_QUEUE.value], timeout=10)
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]

            metadata = {"claim_id": claim_id, "status": "processing"}

            await r.lpush(Queues.OCR_QUEUE.value, json.dumps(metadata))
            logger.info(f"[{claim_id}] is being processed.")


if __name__ == "__main__":
    asyncio.run(worker())
