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
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379))
)

logger = get_logger()
MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))

async def perform_ocr(claim_id: str) -> int:
    # This function mocks the OCR step
    # TODO describe in detail the possible options for OCR
    # eg. Tesseract, Docling
    # Give details about each method, including pros/cons. Possibility to deploy on prem, etc.

    case_document_dir = get_local_storage().file_path(claim_id)
    document_path = case_document_dir / f"{claim_id.lower()}.pdf"

    # 1. Load pdf
    # 2. Perform OCR
    # 3. save the OCR in case_document_dir

    time.sleep(random.randint(1, 4))

    return 1


async def worker():
    while True:
        message = await r.brpop("ocr-queue")
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload['claim_id']
            retries = payload.get("retries", 0)
            try:
                result = await perform_ocr(claim_id)

                if result:
                    metadata = {
                        "claim_id": claim_id,
                        "status": "ocr_performed"
                    }

                    await r.lpush('data-extraction-queue', json.dumps(metadata))
                    logger.info(f"[{claim_id}] is being processed.")
                else:
                    logger.info(f"OCR failed for {claim_id=}.")
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload['retries'] = retries + 1
                    await r.lpush("ocr-queue", json.dumps(payload))
                else:
                    await r.lpush("ocr-dlq", json.dumps(payload))
                    logger.error(f"Added {claim_id=} to OCR DLQ.")

if __name__ == '__main__':
    asyncio.run(worker())