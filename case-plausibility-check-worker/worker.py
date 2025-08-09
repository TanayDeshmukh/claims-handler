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


async def run_case_plausibility_check(claim_id: str) -> bool:
    # This function mocks the plausibility check of the cost positions
    # TODO describe in detail the possible options for cost position extraction
    # eg. Random Forest/ XGBoost, Transformer based models

    case_document_dir = get_local_storage().file_path(claim_id)
    case_df_path = case_document_dir / f"{claim_id.lower()}.parquet"

    # read the cost positions from the database
    # Generate cost position embeddings (Pretrained models in short term, custom trained models in the long run)
    # Concatenate with embeddings with case features
    # Tree based algorithm (Random forest / XGBoost) to predict the plausibility
    # Result can be decided by applying a threshold to the output
    time.sleep(random.randint(1, 5))

    result = random.choices([True, False], [0.8, 0.2], k=1)

    return result[0]


async def worker():
    while True:
        message = await r.brpop("case-plausibility-check-queue")
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                result = await run_case_plausibility_check(claim_id)
                metadata = {
                    "claim_id": claim_id,
                    "status": "cost_positions_extraction_completed",
                }

                if result:
                    await r.lpush("claim-accepted-queue", json.dumps(metadata))
                else:
                    logger.info(f"Rejected {claim_id=}.")
                    # The claim was rejected. The claim can be added to a separate queue for rejection or human feedback
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush("case-plausibility-check-queue", json.dumps(payload))
                else:
                    await r.lpush("case-plausibility-check-dlq", json.dumps(payload))
                    logger.error(f"Added {claim_id=} to case plausibility check DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
