from abc import ABC, abstractmethod
import time
from typing import List
import redis.asyncio as redis



class DB(ABC):

    @abstractmethod
    async def connect(self, host: str, port: int) -> None:
        ...

    @abstractmethod
    async def insert(self, id: str, spider_name: str) -> None:
        ...

    @abstractmethod
    async def exists(self, id: str, spider_name: str) -> bool:
        ...

    @abstractmethod
    async def remove(self, id: str, spider_name: str) -> None:
        ...

    @abstractmethod
    async def cleanup(self, days: int) -> None:
        ...

    @abstractmethod
    async def close(self) -> None:
        ...


class RedisDB(DB):
    settings: List[str] = ["DB_HOST", "DB_PORT", "DB_PASS"]
    PROCESSED_IDS_ZSET: str = "processed:ids:zset"

    async def connect(self, host: str = "localhost", port: int = 6379, password: str = None) -> None:
        self.r = redis.Redis(
            host=host,
            port=port,
            password=password,
            socket_timeout=5,
            socket_connect_timeout=5,
            decode_responses=True,
        )
        await self.r.ping()

    async def insert(self, id: str, spider_name: str) -> None:
        unique_id = f"{spider_name}_{id}"
        timestamp = int(time.time())
        await self.r.zadd(self.PROCESSED_IDS_ZSET, {unique_id: timestamp})

    async def exists(self, id: str, spider_name: str) -> bool:
        unique_id = f"{spider_name}_{id}"
        score = await self.r.zscore(self.PROCESSED_IDS_ZSET, unique_id)
        return score is not None

    async def remove(self, id: str, spider_name: str) -> None:
        unique_id = f"{spider_name}_{id}"
        self.r.delete(unique_id)

    async def cleanup(self, days: int) -> None:
        threshold_timestamp = int(time.time()) - (days * 24 * 60 * 60)
        await self.r.zremrangebyscore(self.PROCESSED_IDS_ZSET, 0, threshold_timestamp)

    async def close(self) -> None:
        if hasattr(self, "r"):
            await self.r.aclose()
