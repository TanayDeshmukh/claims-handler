import asyncio
import json
import os
import random

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


async def run_cost_position_extraction(claim_id: str):
    # This function mocks the cost position extraction step
    # eg. Donut, LLMs, Table Transformer, Amazon Tesseract, Azure Document Intelligence,
    # Docling, pdfplumber (work well for machine generated pdf)

    claim_document_dir = get_local_storage().file_path(claim_id)
    document_path = claim_document_dir / f"{claim_id.lower()}.pdf"
    dummy_ocr_file = claim_document_dir / f"{claim_id.lower()}.txt"

    with open(dummy_ocr_file, "r") as file:
        ocr = file.read().rstrip()

    # Use the PDF and OCR to extract cost positions using one of the mentioned methods
    # The cost positions will ideally be saved in a database

    await asyncio.sleep(random.randint(1, 5))


async def worker():
    while True:
        message = await r.brpop(
            [Queues.COST_POSITIONS_EXTRACTION_QUEUE.value], timeout=10
        )
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                _ = await run_cost_position_extraction(claim_id)
                metadata = {
                    "claim_id": claim_id,
                    "status": "cost_positions_extraction_completed",
                }
                await r.lpush(
                    Queues.CASE_PLAUSIBILITY_CHECK_QUEUE.value, json.dumps(metadata)
                )
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush(
                        Queues.COST_POSITIONS_EXTRACTION_QUEUE.value,
                        json.dumps(payload),
                    )
                else:
                    await r.lpush(
                        Queues.COST_POSITIONS_EXTRACTION_DLQ.value, json.dumps(payload)
                    )
                    logger.error(f"Added {claim_id=} to cost positions extraction DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
