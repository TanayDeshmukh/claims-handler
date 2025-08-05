import sys
import logging
from enum import Enum
from functools import lru_cache

# class Status(Enum):
#     INGESTED = "ingested"

@lru_cache
def get_logger():
    logger = logging.getLogger("ClaimsHandlerLogger")
    if not logger.handlers:
        # Create console handler
        handler = logging.StreamHandler(sys.stdout)

        # Create formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)

        # Add handler to logger
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger