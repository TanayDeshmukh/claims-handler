import os

import redis
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.responses import HTMLResponse

load_dotenv()

app = FastAPI()
r = redis.Redis(
    host=os.getenv('REDIS_HOST', 'redis'),
    port=int(os.getenv('REDIS_PORT', 6379))
)

@app.get("/", response_class=HTMLResponse)
async def read_items():
    queue_names = ['email-ingest-queue', 'pdf-processing-queue']
    statuses = {name: r.llen(name) for name in queue_names}

    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>POC Monitoring Dashboard</title>
        <meta http-equiv="refresh" content="5">
    </head>
    <body>
        <h1>Pipeline Status (Auto-refreshing)</h1>
        <hr>
        <h2>Queue Lengths</h2>
        <ul>
    """
    for queue, length in statuses.items():
        html_content += f"<li><strong>{queue}</strong>: {length} messages</li>"

    html_content += """
        </ul>
        <p>This shows the real-time load on each part of the system. A growing queue indicates a bottleneck.</p>
    </body>
    </html>
    """
    return html_content