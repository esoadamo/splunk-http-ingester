import tomllib
from os import environ
from pathlib import Path
from typing import Set

from dotenv import load_dotenv
from fastapi import FastAPI, Body

from ingest_cache import IngestRequest, IngestCache

load_dotenv()


DIR_DATA = Path(environ.get("SHI_DATA")) if 'SHI_DATA' in environ else Path(__file__).parent / "data"
CONFIG = tomllib.loads((DIR_DATA / "config.toml").read_text())

API_KEYS: Set[str] = set(CONFIG["local"]["api_keys"])
CACHE = IngestCache(DIR_DATA / "cache", CONFIG)


app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Splunk ingestion service is running"}


@app.get("/healthcheck")
async def healthcheck():
    return {"message": "OK"}


@app.post("/ingest")
async def ingest(api_key: str, source_type: str, source: str, channel: str,
                 payload: str = Body(...)):
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
