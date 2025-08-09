import logging
import sys
from functools import lru_cache


# class Status(Enum):
#     INGESTED = "ingested"


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
