import asyncio
from pathlib import Path
from random import randint
from typing import TypedDict, Optional, List, Set
from uuid import uuid4

import httpx
from sqlidictature import SQLiDictature

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
        self.__cache_db = SQLiDictature(cache_dir / "cache.sqlite3")
        self.__cache_last_records = self.__cache_db['_last_records']
        self.__config = config

        asyncio.create_task(self.__send_all_cached())

    async def __send_all_cached(self):
        while True:
            # noinspection PyBroadException
            try:
                await self.send_all_saved()
            except Exception:
                pass
            await asyncio.sleep(300 + randint(0, 300))

    def trim_request(self, ingest_request: IngestRequest) -> IngestRequest:
        last_line = self.__cache_last_records.get(ingest_request['channel'])
        if last_line:
            last_line_index = ingest_request['payload'].find(last_line)
            if last_line_index != -1:
                ingest_request['payload'] = ingest_request['payload'][last_line_index + len(last_line) + 1:]
        self.__cache_last_records[ingest_request['channel']] = get_last_line(ingest_request['payload'])
        return ingest_request

    def save(self, ingest_request: IngestRequest, service: str):
        cache_table = f"cr_{service}_{ingest_request['channel']}"
        self.__cache_db[cache_table][str(uuid4())] = ingest_request

    async def send_all_saved(self) -> None:
        for cache_table in filter(lambda x: x.startswith('cr_'), self.__cache_db.keys()):
            _, service, channel = cache_table.split('_')
            for key, request in self.__cache_db[cache_table].items():
                success = await self.send(request, False, {service})
                if success:
                    del self.__cache_db[cache_table][key]
            if not self.__cache_db[cache_table].keys():
                del self.__cache_db[cache_table]

    async def send(self, ingest_request: IngestRequest, save_on_fail: bool = True, services: Optional[Set[str]] = None) -> bool:
        request = self.trim_request(ingest_request)
        if not request['payload'].strip():
            return True

        result_set = False
        result = True
        if self.__config.get('splunk') and (not services or 'splunk' in services):
            result = result and await self.__send_splunk(request, save_on_fail)
            result_set = True
        if self.__config.get('crystalline') and (not services or 'crystalline' in services):
            result = result and await self.__send_crystalline(request, save_on_fail)
            result_set = True
        if self.__config.get('shi') and (not services or 'shi' in services):
            result = result and await self.__send_shi(request, save_on_fail)
            result_set = True
        return result and result_set

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
