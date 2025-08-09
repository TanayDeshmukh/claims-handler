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


async def run_data_extraction(claim_id: str):
    # This function mocks the data extraction step
    # Models like Custom NER model, LLM, Donut can be used

    claim_document_dir = get_local_storage().file_path(claim_id)
    dummy_ocr_file = claim_document_dir / f"{claim_id.lower()}.txt"

    with open(dummy_ocr_file, "r") as file:
        ocr = file.read().rstrip()

    # LLM call to extract structured information from the dummy ocr text
    # Output can be saved in a parquet file in the claim storage dir

    time.sleep(random.randint(1, 4))


async def worker():
    while True:
        message = await r.brpop([Queues.DATA_EXTRACTION_QUEUE.value], timeout=10)
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                _ = await run_data_extraction(claim_id)

                metadata = {"claim_id": claim_id, "status": "data_extraction_performed"}

                await r.lpush(
                    Queues.POLICY_COVERAGE_CHECK_QUEUE.value, json.dumps(metadata)
                )
                logger.info(f"[{claim_id}] data extraction completed.")

            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush(
                        Queues.DATA_EXTRACTION_QUEUE.value, json.dumps(payload)
                    )
                else:
                    await r.lpush(Queues.DATA_EXTRACTION_DLQ.value, json.dumps(payload))
                    logger.error(f"Added {claim_id=} to data extraction DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
