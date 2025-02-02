import asyncio
from pathlib import Path
from pickle import loads, dumps
from threading import Lock
from typing import TypedDict, Dict, Optional, List

import httpx

from time_utils import extract_timestamp_unix


class IngestRequest(TypedDict):
    source_type: str
    source: str
    channel: str
    payload: str


class ConfigService(TypedDict):
    endpoint: str
    token: str

class ConfigLocal(TypedDict):
    api_keys: List[str]


class Config(TypedDict):
    local: ConfigLocal
    splunk: Optional[ConfigService]
    crystalline: Optional[ConfigService]
    shi: Optional[ConfigService]


def get_last_line(text: str) -> str:
    try:
        return text.strip().splitlines()[-1].strip()
    except IndexError:
        return ""


class IngestCache:
    def __init__(self, cache_dir: Path, config: Config):
        self.__cache_dir: Path = cache_dir
        self.__cache_dir.mkdir(parents=True, exist_ok=True)
        self.__cache_last_records: Dict[str, str] = {}
        self.__cache_last_records_lock = Lock()
        self.__cache_last_records_file = self.__cache_dir / "_last_records.pickle"
        self.__cache_locks: Dict[str, Lock] = {}
        self.__cache_lock = Lock()
        self.__config = config

        if self.__cache_last_records_file.exists():
            self.__cache_last_records = loads(self.__cache_last_records_file.read_bytes())

        asyncio.create_task(self.__persist_loop())
        asyncio.create_task(self.__persist_loop())

    async def __persist_loop(self):
        while True:
            # noinspection PyBroadException
            try:
                self.persist()
            except Exception:
                pass
            await asyncio.sleep(10)

    async def __send_all_cached(self):
        while True:
            # noinspection PyBroadException
            try:
                await self.send_all_saved()
            except Exception:
                pass
            await asyncio.sleep(300)

    def __get_cache_lock(self, channel: str) -> Lock:
        if channel not in self.__cache_locks:
            with self.__cache_lock:
                if channel not in self.__cache_locks:
                    self.__cache_locks[channel] = Lock()
        return self.__cache_locks[channel]

    def persist(self):
        with self.__cache_last_records_lock:
            self.__cache_last_records_file.write_bytes(dumps(self.__cache_last_records))

    def trim_request(self, ingest_request: IngestRequest) -> IngestRequest:
        with self.__get_cache_lock(ingest_request['channel']):
            last_line = self.__cache_last_records.get(ingest_request['channel'])
            if last_line:
                last_line_index = ingest_request['payload'].find(last_line)
                if last_line_index != -1:
                    ingest_request['payload'] = ingest_request['payload'][last_line_index + len(last_line) + 1:]
            self.__cache_last_records[ingest_request['channel']] = get_last_line(ingest_request['payload'])
        return ingest_request

    def save(self, ingest_request: IngestRequest, service: str):
        cache_file = self.__cache_dir / f"{service}_{ingest_request['channel']}.pickle"
        with self.__get_cache_lock(ingest_request['channel']):
            existing = loads(cache_file.read_bytes()) if cache_file.exists() else []
            existing.append(ingest_request)
            cache_file.write_bytes(dumps(existing))

    async def send_all_saved(self) -> None:
        for cache_file in self.__cache_dir.glob("*.pickle"):
            if cache_file.name.startswith("_"):
                continue
            with self.__get_cache_lock(cache_file.stem):
                if not cache_file.exists():
                    continue
                requests = loads(cache_file.read_bytes())
                for request in requests:
                    if await self.send(request, save_on_fail=False):
                        requests.remove(request)
                cache_file.write_bytes(dumps(requests))

    async def send(self, ingest_request: IngestRequest, save_on_fail: bool = True) -> bool:
        request = self.trim_request(ingest_request)
        result = True
        if self.__config.get('splunk'):
            result = result and await self.__send_splunk(request, save_on_fail)
        if self.__config.get('crystalline'):
            result = result and await self.__send_crystalline(request, save_on_fail)
        if self.__config.get('shi'):
            result = result and await self.__send_shi(request, save_on_fail)
        return result

    async def __send_splunk(self, request: IngestRequest, save_on_fail: bool = True) -> bool:
        async with httpx.AsyncClient(verify=False, timeout=360.0) as client:
            headers = {
                'Authorization': f'Splunk {self.__config["splunk"]["token"]}',
                'Content-Type': 'text/plain',
                'X-Splunk-Request-Channel': request['channel'],
            }
            response = await client.post(
                self.__config['splunk']['endpoint'],
                content=request['payload'],
                params={
                    "sourcetype": request['source_type'],
                    "source": request['source'],
                },
                headers=headers
            )

            if response.status_code != 200:
                if save_on_fail:
                    self.save(request, 'splunk')
                return False
            return True

    async def __send_crystalline(self, request: IngestRequest, save_on_fail: bool = True) -> bool:
        async with httpx.AsyncClient(verify=False, timeout=360.0) as client:
            headers = {
                'X-Crystalline-Token': self.__config['crystalline']['token'],
                'Content-Type': 'text/plain',
            }

            lines = request['payload'].splitlines()
            lines = map(
                lambda x: f"timestamp={extract_timestamp_unix(x)} "
                          f"source={request['source']} "
                          f"sourcetype={request['source_type']} "
                          f"{x}",
                lines
            )

            response = await client.post(
                f"{self.__config["crystalline"]["endpoint"]}/raw",
                content="\n".join(lines),
                headers=headers
            )

            if response.status_code != 200:
                if save_on_fail:
                    self.save(request, 'crystalline')
                return False
            return True

    async def __send_shi(self, request: IngestRequest, save_on_fail: bool = True) -> bool:
        async with httpx.AsyncClient(verify=False, timeout=360.0) as client:
            headers = {
                'Content-Type': 'text/plain',
            }

            response = await client.post(
                f"{self.__config["shi"]["endpoint"]}",
                content=request['payload'],
                params={
                    "source": request['source'],
                    "source_type": request['source_type'],
                    "channel": request['channel'],
                    "api_key": self.__config["shi"]["token"]
                },
                headers=headers
            )

            if response.status_code != 200:
                if save_on_fail:
                    self.save(request, 'shi')
                return False
            return True