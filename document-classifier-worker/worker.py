import asyncio
import json
import os
import random
import time
from typing import Literal

import redis.asyncio as redis
from dotenv import load_dotenv

from common.storage import get_local_storage
from common.utils import get_logger, Queues

load_dotenv()


r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379))
)

logger = get_logger()
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))


async def classify_document(claim_id: str) -> Literal["partial", "total_loss", "other"]:
    # This function mocks the document classification step

    claim_document_dir = get_local_storage().file_path(claim_id)
    dummy_ocr_file = claim_document_dir / f"{claim_id.lower()}.txt"

    with open(dummy_ocr_file, "r") as file:
        ocr = file.read().rstrip()

    # LLM call to classify the document type

    time.sleep(random.randint(1, 4))
    document_type = random.choices(
        ["partial", "total_loss", "other"], [0.8, 0.1, 0.1], k=1
    )

    return document_type[0]


async def worker():
    while True:
        message = await r.brpop([Queues.DOCUMENT_CLASSIFIER_QUEUE.value], timeout=10)
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                document_type = await classify_document(claim_id)
                metadata = {
                    "claim_id": claim_id,
                    "status": f"document_type_{document_type}",
                }

                if document_type == "partial":
                    await r.lpush(
                        Queues.DATA_EXTRACTION_QUEUE.value, json.dumps(metadata)
                    )
                    logger.info(f"[{claim_id}] classified as {document_type=}.")
                else:
                    logger.error(
                        f"[{claim_id}] is of {document_type=}. Can not be processed further."
                    )
                    await r.lpush(
                        Queues.CLAIM_REJECTION_QUEUE.value, json.dumps(metadata)
                    )
                    # the logic to reject claim/ handover to the user/ send automated reply back to the sender, goes here.
                    # The claim can be added to a separate queue for human intervention
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush(
                        Queues.DOCUMENT_CLASSIFIER_QUEUE.value, json.dumps(payload)
                    )
                else:
                    await r.lpush(
                        Queues.DOCUMENT_CLASSIFIER_DLQ.value, json.dumps(payload)
                    )
                    logger.error(f"Added {claim_id=} to document classifier DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
