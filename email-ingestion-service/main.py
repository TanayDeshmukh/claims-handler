import json
import os

import redis.asyncio as redis
from dotenv import load_dotenv
from fastapi import FastAPI, Form, UploadFile, File
from fastapi.responses import JSONResponse

from common.storage import get_local_storage, LocalStorage
from common.utils import get_logger

load_dotenv()

app = FastAPI()
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379))
)

@app.post("/ingest-email")
async def ingest_email(
        sender: str = Form(...),
        subject: str = Form(...),
        body: str = Form(...),
        attachment: UploadFile = File(...)
):
    logger = get_logger()

    logger.info(f"EMAIL RECEIVED: {sender} {subject}")

    local_storage: LocalStorage = get_local_storage()
    pdf_contents = await attachment.read()

    logger.info(f"Read bytes size: {len(pdf_contents)}")
    logger.info(f"First 10 bytes: {pdf_contents[:10]}")

    claim_id = await local_storage.store(file_bytes=pdf_contents, extension='pdf')

    metadata = {
        "claim_id": claim_id,
        "status": "Ingested"
    }

    await r.lpush("email-ingest-queue", json.dumps(metadata))
    logger.info(f"Email ingested and added to queue with ID {claim_id}")

    return JSONResponse(content={"message": "Email ingested", "claim_id": claim_id})