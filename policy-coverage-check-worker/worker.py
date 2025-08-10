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


async def run_policy_coverage_check(claim_id: str) -> int:
    # This function mocks the policy coverage check step
    # Lookup table
    # Fuzzy check
    # Reranker

    claim_document_dir = get_local_storage().file_path(claim_id)
    case_df_path = claim_document_dir / f"{claim_id.lower()}.parquet"

    time.sleep(random.randint(1, 5))

    result = random.choices([True, False], [0.8, 0.2], k=1)

    return result[0]


async def worker():
    while True:
        message = await r.brpop([Queues.POLICY_COVERAGE_CHECK_QUEUE.value], timeout=10)
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload["claim_id"]
            retries = payload.get("retries", 0)
            try:
                result = await run_policy_coverage_check(claim_id)
                metadata = {
                    "claim_id": claim_id,
                    "status": f"policy_coverage_check_{str(result).lower()}",
                }
                if result:
                    await r.lpush(
                        Queues.COST_POSITIONS_EXTRACTION_QUEUE.value,
                        json.dumps(metadata),
                    )
                    logger.info(f"[{claim_id}] policy verified.")

                else:
                    logger.error(f"Policy check for {claim_id=} returned false.")
                    await r.lpush(
                        Queues.CLAIM_REJECTION_QUEUE.value, json.dumps(metadata)
                    )
                    # The claim is not eligible under the policy. The claim can be added to a separate queue for rejection or human feedback
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload["retries"] = retries + 1
                    await r.lpush(
                        Queues.POLICY_COVERAGE_CHECK_QUEUE.value, json.dumps(payload)
                    )
                else:
                    await r.lpush(
                        Queues.POLICY_COVERAGE_CHECK_DLQ.value, json.dumps(payload)
                    )
                    logger.error(f"Added {claim_id=} to policy coverage check DLQ.")


if __name__ == "__main__":
    asyncio.run(worker())
