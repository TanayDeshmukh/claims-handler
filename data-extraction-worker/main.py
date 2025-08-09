import asyncio
import json
import os
import random
import time
from typing import Literal

import redis.asyncio as redis
from dotenv import load_dotenv

from common.storage import get_local_storage
from common.utils import get_logger

load_dotenv()


r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379))
)

logger = get_logger()
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))


async def run_data_extraction(claim_id: str):
    # This function mocks the data extraction step
    # TODO describe in detail the possible options for data extraction
    # eg. Custom NER model, LLM, Donut
    # Give details about each method, including pros/cons. Training details.

    case_document_dir = get_local_storage().file_path(claim_id)
    ocr_path = case_document_dir / f"{claim_id.lower()}.xml"

    # 1. Load OCR
    # 2. Run data extraction model
    # 3. save the extracted data in case_document_dir is a json, parquet, etc. format.

    time.sleep(random.randint(1, 4))


async def worker():
    while True:
        message = await r.brpop("data-extraction-queue")
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                _ = await run_data_extraction(claim_id)

                metadata = {"claim_id": claim_id, "status": "data_extraction_performed"}

                await r.lpush("policy-coverage-check-queue", json.dumps(metadata))
                logger.info(f"[{claim_id}] is being processed.")

            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush("data-extraction-queue", json.dumps(payload))
                else:
                    await r.lpush("data-extraction-dlq", json.dumps(payload))
                    logger.error(f"Added {claim_id=} to data extraction DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
