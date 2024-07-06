from os import environ
from pathlib import Path
from typing import Set, Optional
from uuid import uuid4

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Body

load_dotenv()

SPLUNK_ENDPOINT = environ["SHI_ENDPOINT"]
SPLUNK_TOKEN = environ["SHI_TOKEN"]
DIR_DATA = Path(environ.get("SHI_DATA")) if 'SHI_DATA' in environ else Path(__file__).parent / "data"
FILE_API_KEYS = DIR_DATA / "api_keys.txt"

API_KEYS: Set[str] = set(filter(bool, map(str.strip, FILE_API_KEYS.read_text().splitlines())))

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Splunk ingestion service is running"}


@app.get("/healthcheck")
async def healthcheck():
    return {"message": "OK"}


@app.post("/ingest")
async def ingest(api_key: str, source_type: str, source: str,
                 payload: str = Body(..., media_type="text/plain"), channel: Optional[str] = None):
    if api_key not in API_KEYS:
        return {"message": "Invalid API key"}, 401

    if not channel:
        channel = uuid4().hex.upper()

    async with httpx.AsyncClient(verify=False) as client:
        headers = {
            'Authorization': f'Splunk {SPLUNK_TOKEN}',
            'Content-Type': 'text/plain',
            'X-Splunk-Request-Channel': channel
        }
        response = await client.post(
            SPLUNK_ENDPOINT,
            content=payload,
            params={
                "sourcetype": source_type,
                "source": source,
            },
            headers=headers
        )

        if response.status_code == 200:
            return {"message": "Data ingested and forwarded to Splunk"}
        else:
            return {"message": "Failed to forward data to Splunk", "error": response.text}, 500
