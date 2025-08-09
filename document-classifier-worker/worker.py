import asyncio
import json
import os
import random
import time
from typing import Literal

import redis.asyncio as redis
from dotenv import load_dotenv

from common.utils import get_logger

load_dotenv()


r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379))
)

logger = get_logger()
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))


async def classify_document(claim_id: str) -> Literal["partial", "total_loss", "other"]:
    # This function mocks the document classification step
    # TODO describe the method in detail including all the possible options
    # eg. Simple CNN, Vision Transformer, LLM, keyword extraction (needs OCR)
    # Give details about each method, including training requirements

    time.sleep(random.randint(1, 4))
    document_type = random.choices(
        ["partial", "total_loss", "other"], [0.8, 0.1, 0.1], k=1
    )

    return document_type[0]


async def worker():
    while True:
        message = await r.brpop("document-classifier-queue")
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                document_type = await classify_document(claim_id)

                if document_type == "partial":
                    metadata = {"claim_id": claim_id, "status": "document_classified"}

                    await r.lpush("ocr-queue", json.dumps(metadata))
                    logger.info(f"[{claim_id}] is being processed.")
                else:
                    logger.error(
                        f"[{claim_id}] is of {document_type=}. Can not be processed further."
                    )
                    # the logic to reject claim/ handover to the user/ send automated reply back to the sender, goes here.
                    # The claim can be added to a separate queue for human intervention
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush("document-classifier-queue", json.dumps(payload))
                else:
                    await r.lpush("document-classifier-dlq", json.dumps(payload))
                    logger.error(f"Added {claim_id=} to document classifier DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
