import logging
import sys
from enum import Enum
from functools import lru_cache


@lru_cache
def get_logger():
    logger = logging.getLogger("ClaimsHandlerLogger")
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


class Queues(Enum):
    EMAIL_INGESTION_QUEUE = "email-ingestion-queue"

    OCR_QUEUE = "ocr-queue"
    OCR_DLQ = "ocr-dlq"

    DOCUMENT_CLASSIFIER_QUEUE = "document-classifier-queue"
    DOCUMENT_CLASSIFIER_DLQ = "document-classifier-dlq"

    DATA_EXTRACTION_QUEUE = "data-extraction-queue"
    DATA_EXTRACTION_DLQ = "data-extraction-dlq"

    POLICY_COVERAGE_CHECK_QUEUE = "policy-coverage-check-queue"
    POLICY_COVERAGE_CHECK_DLQ = "policy-coverage-check-dlq"

    COST_POSITIONS_EXTRACTION_QUEUE = "cost-positions-extraction-queue"
    COST_POSITIONS_EXTRACTION_DLQ = "cost-positions-extraction-dlq"

    CASE_PLAUSIBILITY_CHECK_QUEUE = "case-plausibility-check-queue"
    CASE_PLAUSIBILITY_CHECK_DLQ = "case-plausibility-check-dlq"

    CLAIM_ACCEPTANCE_QUEUE = "claim-acceptance-queue"
    CLAIM_REJECTION_QUEUE = "claim-rejection-queue"
