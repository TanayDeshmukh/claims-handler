import os
import redis
import json
import time
from dotenv import load_dotenv

load_dotenv()


r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379))
)

while True:
    message = r.brpop("email-ingest-queue")
    if message:
        queue_name, data = message
        payload = json.loads(data)
        correlation_id = payload['correlation_id']
        email_data = payload['payload']

        processed_data = {
            "correlation_id": correlation_id,
            "body": email_data['body'],
            "attachments": [att for att in email_data['attachments'] if att['filename'].endswith('.pdf')]
        }

        r.lpush('pdf-processing-queue', json.dumps(processed_data))
        print(f"[{correlation_id}] Email processed. Pushed to PDF queue.")
    time.sleep(1)