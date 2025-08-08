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


async def run_policy_coverage_check(claim_id: str) -> int:
    # This function mocks the policy coverage check step
    # TODO describe in detail the possible options for policy coverage check
    # Lookup table
    # Fuzzy check
    # Reranker
    # Give details about each method, including pros/cons.

    case_document_dir = get_local_storage().file_path(claim_id)
    case_df_path = case_document_dir / f"{claim_id.lower()}.parquet"

    # 1. Load dataframe
    # 2. Use lookup table for hard checks like policy number and damage date. (Must have a 100% match)
    # 3. Use fuzzy check for names, shop details, bicycle details. (Typos are common when writing names, hence a fuzzy check)
    # 4. Use transformer based reranker model to check similarity of damage description to the policy description. (damage description should match the policy description)
    # 5. Calculate a confidence using the three methods
    # 6. Return a boolean flag by thresholding the confidence

    time.sleep(random.randint(1, 5))

    result = random.choices([True, False], [0.8, 0.2], k=1)

    return result[0]

async def worker():
    while True:
        message = await r.brpop("policy-coverage-check-queue")
        if message:
            queue_name, data = message
            payload = json.loads(data)
            claim_id = payload['claim_id']
            retries = payload.get("retries", 0)
            try:
                result = await run_policy_coverage_check(claim_id)

                if result:
                    metadata = {
                        "claim_id": claim_id,
                        "status": "policy_coverage_check_performed"
                    }

                    await r.lpush('cost-positions-extraction-queue', json.dumps(metadata))
                else:
                    logger.info(f"Policy check for {claim_id=} returned false.")
                    # The claim is not eligible under the policy. The claim can be added to a separate queue for rejection or human feedback
            except Exception as e:
                if retries < MAX_RETRIES:
                    payload['retries'] = retries + 1
                    await r.lpush("policy-coverage-check-queue", json.dumps(payload))
                else:
                    await r.lpush("policy-coverage-check-dlq", json.dumps(payload))
                    logger.error(f"Added {claim_id=} to policy coverage check DLQ.")


if __name__ == '__main__':
    asyncio.run(worker())