from functools import wraps
import importlib
import json
from typing import Dict, Callable, List, Self
import grpc
import scrapy
from scrapy.crawler import Crawler
from scrapy.spiders import Spider
from scrapy.exceptions import DropItem, NotConfigured
from tinydb import TinyDB, Query
from datetime import datetime, timedelta
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.http.request import NO_CALLBACK
import dateparser
from twisted.internet.threads import deferToThread
from twisted.internet.defer import Deferred
from scrapy import signals
import websockets



class PreProcessingPipeline:
    """
    Pipeline to preprocess items before forwarding.
    Handles deduplication, date filtering, and data cleaning.

    Attributes:
        file_path (str): Path to TinyDB database file. Defaults to "db.json"
        expiry_days (int): Number of days to keep records. Defaults to 60
    """

    def __init__(self, file_path: str = "db.json", expiry_days: int = 60) -> None:
        self.file_path = file_path
        self.expiry_days = expiry_days

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = []
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            file_path=crawler.settings.get("PREPROCESSING_DB_PATH"),
            expiry_days=crawler.settings.get("PREPROCESSING_EXPIRY_DAYS"),
        )

    def open_spider(self, spider: Spider) -> None:
        self._init_db()

    def close_spider(self, spider: Spider) -> None:
        if hasattr(self, "db"):
            self._db.close()

    def _init_db(self) -> None:
        self._db = TinyDB(self.file_path)
        self._query = Query()
        self._cleanup_old_records()

    def tinydb_insert(self, id: str) -> None:
        self._db.insert({"id": id, "timestamp": datetime.now().isoformat()})

    def tinydb_exists(self, id: str) -> bool:
        return bool(self._db.search(self._query.id == id))

    def _cleanup_old_records(self) -> None:
        cutoff_date = datetime.now() - timedelta(days=60)
        cutoff_str = cutoff_date.isoformat()
        self._db.remove(self._query.timestamp < cutoff_str)

    @staticmethod
    def is_today(date_str: str, date_format: str = None) -> bool:
        if not date_str:
            return True
        today = datetime.now().date()
        input_date = dateparser.parse(date_string=date_str, date_formats=[date_format] if date_format is not None else None).date()
        if today == input_date:
            return True
        else:
            return False

    @staticmethod
    def clean_item(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(self, item: Dict, spider: Spider) -> Dict:
            cleaned_item = {}
            for k,v in item.items():
                if isinstance(v, str):
                    v = "\n".join([" ".join(line.split()) for line in v.splitlines()])
                cleaned_item[k] = v
            return func(self, cleaned_item, spider)
        return wrapper

    @clean_item
    def process_item(self, item: Dict, spider: Spider) -> Dict:
        _id = item.get("_id", None)
        if _id:
            if self.tinydb_exists(id=_id):
                raise DropItem(f"Already exists [{_id}]")
            self.tinydb_insert(id=_id)
        _dt = item.pop("_dt", None)
        _dt_format = item.pop("_dt_format", None)
        if _dt:
            if not self.is_today(_dt, _dt_format):
                raise DropItem(f"Outdated [{_dt}]")

        if not {k: v for k, v in item.items() if not k.startswith("_") and v}:
            raise DropItem("Item keys have None values!")
        return item
    


class DiscordPipeline:
    """
    Pipeline to send items to a Discord webhook.

    Attributes:
        uri (str):
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = ["body"]

    def __init__(self, uri: str) -> None:
        self.uri = uri

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["DISCORD_SERVER_URI"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            uri=crawler.settings.get("DISCORD_SERVER_URI")
        )

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        await maybe_deferred_to_future(
            spider.crawler.engine.download(
                scrapy.Request(
                    url=self.uri,
                    method="POST",
                    body=json.dumps(_item),
                    headers={"Content-Type": "application/json"},
                    callback=NO_CALLBACK,
                ),
            )
        )



class SynopticPipeline:
    """
    Pipeline to send items to a Synoptic stream.

    Attributes:
        stream_id (str):
        api_key (str):
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = []

    def __init__(self, uri: str, stream_id: str, api_key: str) -> None:
        self.uri = uri
        self.stream_id = stream_id
        self.api_key = api_key

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["SYNOPTIC_SERVER_URI","SYNOPTIC_STREAM_ID","SYNOPTIC_API_KEY"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            uri=crawler.settings.get("SYNOPTIC_SERVER_URI"),
            stream_id=crawler.settings.get("SYNOPTIC_STREAM_ID"),
            api_key=crawler.settings.get("SYNOPTIC_API_KEY")
        )

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        await maybe_deferred_to_future(
            spider.crawler.engine.download(
                scrapy.Request(
                    url=self.uri,
                    body=json.dumps(_item),
                    method="POST",
                    headers={"content-type": "application/json", 'x-api-key': self.api_key},
                    callback=NO_CALLBACK
                )
            )
        )



class TelegramPipeline:
    """
    Pipeline to send items to a Telegram channel.

    Attributes:
        uri (str):
        token (str):
        chat_id (str):
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = []

    def __init__(self,  uri: str, token: str, chat_id: str) -> None:
        self.uri = uri
        self.token = token
        self.chat_id = chat_id

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["TELEGRAM_SERVER_URI","TELEGRAM_TOKEN","TELEGRAM_CHAT_ID"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            uri=crawler.settings.get("TELEGRAM_SERVER_URI"),
            token=crawler.settings.get("TELEGRAM_TOKEN"),
            chat_id=crawler.settings.get("TELEGRAM_CHAT_ID"),
        )

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item
    
    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        await maybe_deferred_to_future(
            spider.crawler.engine.download(
                scrapy.Request(
                    url=self.uri,
                    body=json.dumps(_item),
                    method="POST",
                    headers={"content-type": "application/json", 'authorization': self.token},
                    callback=NO_CALLBACK
                )
            )
        )



class GRPCPipeline:
    """
    Pipeline to send items to a gRPC server.

    Attributes:
        uri (str):
        token (str):
        id (str):
        proto_module (str): dotted path to gRPC contract module
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = []

    def __init__(self, uri: str, token: str, id: str, proto_module: str) -> None:
        self.uri = uri
        self.token = token
        self.id = id
        self.feed_pb2 = importlib.import_module(f"{proto_module}.feed_pb2")
        self.feed_pb2_grpc = importlib.import_module(f"{proto_module}.feed_pb2_grpc")
        # gRPC channel is thread-safe
        self._channel_grpc = grpc.secure_channel(self.uri, grpc.ssl_channel_credentials())

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["GRPC_SERVER_URI","GRPC_TOKEN","GRPC_ID","GRPC_PROTO_MODULE"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            uri=crawler.settings.get("GRPC_SERVER_URI"),
            token=crawler.settings.get("GRPC_TOKEN"),
            id=crawler.settings.get("GRPC_ID"),
            proto_module=crawler.settings.get("GRPC_PROTO_MODULE")
        )

    def process_item(self, item: Dict, spider: Spider) -> Deferred:
        d = self._send(item, spider)
        return d
    
    def _send(self, item: Dict, spider: Spider) -> Deferred:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        feed_message = self.feed_pb2.FeedMessage(
            token=self.token,
            feedId=self.id,
            messageId=item['_id'],
            message=json.dumps(_item)
        )
        def _on_success(result) -> Dict:
            spider.logger.debug(f"Sent to gRPC server: {item['_id']}")
            return item
        d = deferToThread(self._submit, feed_message)
        d.addCallback(_on_success)
        d.addErrback(lambda f: spider.logger.error(f"Failed to send to gRPC server: {item['_id']}\n{f.value}"))
        return d

    def _submit(self, feed_message) -> None:
        try:
            # gRPC clients are lightweight, don't need to be cached or reused
            client = self.feed_pb2_grpc.IngressServiceStub(self._channel_grpc)
            client.SubmitFeedMessage(feed_message)
        except grpc.RpcError as e:
            raise e

    def close_spider(self, spider: Spider) -> None:
        self._channel_grpc.close()



class WSPipeline:
    """
    Pipeline to send items to a websocket server.

    Attributes:
        uri (str):
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = []

    def __init__(self, uri: str) -> None:
        self.uri = uri

    @classmethod
    def from_crawler(cls, crawler) -> Self:
        settings = ["WS_SERVER_URI"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        p = cls(
            uri=crawler.settings.get("WS_SERVER_URI")
        )
        crawler.signals.connect(p.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(p.spider_closed, signal=signals.spider_closed)
        return p

    async def spider_opened(self, spider: Spider) -> None:
        self.client = await websockets.connect(self.ws_server_uri)

    async def spider_closed(self, spider: Spider) -> None:
        await self.client.close()

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item
    
    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        try:
            await self.client.send(json.dumps(_item))
            spider.logger.debug(f"Sent to WS server: {item["_id"]}")
        except Exception as e:
            spider.logger.error(f"Failed to send to WS server: {item['_id']}\n{str(e)}")



class HttpPipeline:
    """
    Pipeline to send items to a custom http webhook.

    Attributes:
        uri (str):
        token (str):
        exclude_fields (List[str]): List of fields that needs to be excluded for this pipeline
    """
    exclude_fields: List[str] = []

    def __init__(self, uri: str, token: str) -> None:
        self.uri = uri
        self.token = token

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["HTTP_SERVER_URI","HTTP_TOKEN"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        p = cls(
            uri=crawler.settings.get("HTTP_SERVER_URI"),
            token=crawler.settings.get("HTTP_TOKEN")
        )
        return p

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item
    
    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {k:v for k,v in item.items() if not k.startswith("_") and k.lower() not in self.exclude_fields}
        await maybe_deferred_to_future(
            spider.crawler.engine.download(
                scrapy.Request(
                    url=self.uri,
                    body=json.dumps(_item),
                    method="POST",
                    headers={"content-type": "application/json","authorization":self.token},
                    callback=NO_CALLBACK
                )
            )
        )