from os import environ
from pathlib import Path
from typing import Set

from dotenv import load_dotenv
from fastapi import FastAPI, Body

from ingest_cache import IngestRequest, IngestCache

load_dotenv()

SPLUNK_ENDPOINT = environ["SHI_ENDPOINT"]
SPLUNK_TOKEN = environ["SHI_TOKEN"]
DIR_DATA = Path(environ.get("SHI_DATA")) if 'SHI_DATA' in environ else Path(__file__).parent / "data"
FILE_API_KEYS = DIR_DATA / "api_keys.txt"

API_KEYS: Set[str] = set(filter(bool, map(str.strip, FILE_API_KEYS.read_text().splitlines())))
CACHE = IngestCache(DIR_DATA / "cache", SPLUNK_ENDPOINT, SPLUNK_TOKEN)


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Splunk ingestion service is running"}


@app.get("/healthcheck")
async def healthcheck():
    return {"message": "OK"}


@app.post("/ingest")
async def ingest(api_key: str, source_type: str, source: str, channel: str,
                 payload: str = Body(..., media_type="text/plain")):
    if api_key not in API_KEYS:
        return {"message": "Invalid API key"}, 401

    request: IngestRequest = {
        "source_type": source_type,
        "source": source,
        "channel": channel,
        "payload": payload
    }

    if await CACHE.send(request):
        return {"message": "Data ingested and forwarded to Splunk"}
    else:
        return {"message": "Failed to forward data to Splunk, will send later"}
