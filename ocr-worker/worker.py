import asyncio
import json
import os
import random

import pymupdf
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


async def perform_ocr(claim_id: str):
    # This function mocks the OCR step
    # Models like Tesseract, EasyOCR can be used

    claim_document_dir = get_local_storage().file_path(claim_id)
    document_path = claim_document_dir / f"{claim_id.lower()}.pdf"

    document = pymupdf.open(document_path)
    doc_text = "\n".join([page.get_text() for page in document])
    dummy_ocr_file = claim_document_dir / f"{claim_id.lower()}.txt"

    with open(dummy_ocr_file, "w") as f:
        f.writelines(doc_text)

    await asyncio.sleep(random.randint(1, 5))


async def worker():
    while True:
        message = await r.brpop([Queues.OCR_QUEUE.value], timeout=10)
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                _ = await perform_ocr(claim_id)

                metadata = {"claim_id": claim_id, "status": "ocr_performed"}

                await r.lpush(
                    Queues.DOCUMENT_CLASSIFIER_QUEUE.value, json.dumps(metadata)
                )
                logger.info(f"[{claim_id}] is being processed.")
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush(Queues.OCR_QUEUE.value, json.dumps(payload))
                else:
                    await r.lpush(Queues.OCR_DLQ.value, json.dumps(payload))
                    logger.error(f"Added {claim_id=} to OCR DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
