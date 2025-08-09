import json
import os
import random
import time
from contextlib import asynccontextmanager

import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI, Form, UploadFile, File
from fastapi.responses import JSONResponse
from prometheus_client import start_http_server, Counter, Histogram, Gauge

from common.storage import get_local_storage, LocalStorage
from common.utils import get_logger

load_dotenv()


EMAILS_INGESTED_TOTAL = Counter(
    "emails_ingested_total", "The total number of emails ingested."
)

EMAILS_INGESTION_LATENCY = Histogram(
    "emails_ingestion_latency", "Latency of the email ingestion process in seconds."
)

INGESTION_QUEUE_LENGTH = Gauge(
    "ingestion_queue_length", "Current length of the email ingestion queue."
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    prometheus_server_port = os.getenv("PROMETHEUS_SERVER_PORT")
    start_http_server(int(prometheus_server_port))
    get_logger().info(f"Prometheus metrics started on port  {prometheus_server_port}")

    yield


app = FastAPI(lifespan=lifespan)
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"), port=int(os.getenv("REDIS_PORT", 6379))
)


@app.post("/ingest-email")
async def ingest_email(
    sender: str = Form(...),
    subject: str = Form(...),
    body: str = Form(...),
    attachment: UploadFile = File(...),
):
    with EMAILS_INGESTION_LATENCY.time():
        logger = get_logger()

        logger.info(f"INGESTION PIPELINE [EMAIL RECEIVED]: {sender} {subject}")

        local_storage: LocalStorage = get_local_storage()
        pdf_contents = await attachment.read()

        time.sleep(random.randint(1, 5))

        claim_id = await local_storage.store(file_bytes=pdf_contents, extension="pdf")

        metadata = {"claim_id": claim_id, "status": "ingested"}

        await r.lpush("email-ingest-queue", json.dumps(metadata))
        logger.info(f"Email ingested and added to queue with ID {claim_id}")

        EMAILS_INGESTED_TOTAL.inc()
        queue_length = await r.llen("email-ingest-queue")
        INGESTION_QUEUE_LENGTH.set(queue_length)

        return JSONResponse(content={"message": "Email ingested", "claim_id": claim_id})
