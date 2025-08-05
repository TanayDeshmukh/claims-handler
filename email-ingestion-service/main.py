import os
import redis
import json
import uuid
from fastapi import FastAPI
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()
r = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=int(os.getenv("REDIS_PORT", 6379))
)

class EmailPayload(BaseModel):
    sender: str
    subject: str
    body: str
    attachments: list

@app.post("/ingest-email")
def ingest_email(payload: EmailPayload):
    case_id = str(uuid.uuid4())
    message = {
        "case_id": case_id,
        "payload": payload.model_dump_json()
    }
    r.lpush("email-ingest-queue", json.dumps(message))
    print(f"[{case_id}] Email received and pushed to ingest queue.")
    return {"message": "Email ingested", "case_id": case_id}