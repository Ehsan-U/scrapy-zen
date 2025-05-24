import asyncio
from collections import defaultdict
import importlib
import json
from typing import Dict, List, Self
import grpc
import scrapy
from scrapy.crawler import Crawler
from scrapy.settings import Settings
from scrapy.spiders import Spider
from scrapy.exceptions import DropItem, NotConfigured
from datetime import datetime, timedelta
from scrapy.utils.defer import maybe_deferred_to_future
from scrapy.http.request import NO_CALLBACK
import dateparser
from scrapy import Item, signals
import websockets
import psycopg
from zoneinfo import ZoneInfo
import logging

logging.getLogger("websockets").setLevel(logging.WARNING)

from spidermon.contrib.scrapy.pipelines import ItemValidationPipeline
from scrapy_zen import normalize_url



class PreProcessingPipeline(ItemValidationPipeline):
    """
    Pipeline to preprocess items before forwarding.
    Handles item validation, deduplication, filtering, and cleaning.
    """

    def __init__(
        self,
        settings: Settings,
        validation_enabled: bool,
        validators=None,
        stats=None,
        logger=None,
    ) -> None:
        if validation_enabled:
            super().__init__(
                validators=validators,
                stats=stats,
                drop_items_with_errors=settings.getbool(
                    "SPIDERMON_VALIDATION_DROP_ITEMS_WITH_ERRORS", False
                ),
                add_errors_to_items=settings.getbool(
                    "SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS", False
                ),
                errors_field=settings.get(
                    "SPIDERMON_VALIDATION_ERRORS_FIELD", "_validation"
                ),
            )
        self.settings = settings
        self.validation_enabled = validation_enabled
        self.spider_logger = logger


    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASS"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        spidermon_enabled = crawler.settings.getbool("SPIDERMON_ENABLED")
        if not spidermon_enabled:
            return cls(
                settings=crawler.settings,
                validation_enabled=False,
            )

        validators = defaultdict(list)

        def set_validators(loader, schema):
            if type(schema) in (list, tuple):
                schema = {Item: schema}
            for obj, paths in schema.items():
                key = obj.__name__
                paths = paths if type(paths) in (list, tuple) else [paths]
                objects = [loader(v) for v in paths]
                validators[key].extend(objects)

        schema = crawler.settings.get("SPIDERMON_VALIDATION_SCHEMAS")
        if schema:
            set_validators(cls._load_jsonschema_validator, schema)
        else:
            crawler.spider.logger.warning("No schema defined. Validation disabled")

        p = cls(
            settings=crawler.settings,
            validation_enabled=True if validators else False,
            validators=validators,
            stats=crawler.stats,
            logger=crawler.spider.logger,
        )
        crawler.signals.connect(p.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(p.spider_closed, signal=signals.spider_closed)
        return p

    async def spider_opened(self, spider: Spider) -> None:
        try:
            self.conn = await psycopg.AsyncConnection.connect(f"""
                dbname={self.settings.get("DB_NAME")}
                user={self.settings.get("DB_USER")}
                password={self.settings.get("DB_PASS")}
                host={self.settings.get("DB_HOST")}
                port={self.settings.get("DB_PORT")}
            """)
        except:
            raise NotConfigured("Failed to connect to DB")
        self.cursor = self.conn.cursor()
        await self.cursor.execute("""CREATE TABLE IF NOT EXISTS Items (
            id TEXT PRIMARY KEY,
            spider TEXT,
            scraped_at TEXT,
            published_at TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await self.conn.commit()
        days = self.settings.getint("DB_EXPIRY_DAYS")
        if days:
            spider.logger.warning("Expiration enabled for DB records")
            await self._cleanup_old_records(days)

    async def spider_closed(self, spider: Spider) -> None:
        if hasattr(self, "conn"):
            await self.cursor.close()
            await self.conn.close()

    async def db_insert(self, id: str, spider_name: str, item: Dict) -> None:
        try:
            await self.cursor.execute(
                "INSERT INTO Items (id,spider,scraped_at,published_at) VALUES (%s,%s,%s,%s)", (id, spider_name, item.get("scraped_at"), item.get("published_at"))
            )
        except Exception as e:
            self.spider_logger.error(f"db_insert: {e}")
            raise DropItem(f"Already exists [{id}]")
        else:
            await self.conn.commit()

    async def db_exists(self, id: str, spider_name: str) -> bool:
        try:
            await self.cursor.execute(
                "SELECT id FROM Items WHERE id=%s AND spider=%s", (id, spider_name)
            )
            record = await self.cursor.fetchone()
            return record is not None
        except Exception as e:
            self.spider_logger.error(f"db_exists: {e}")

    async def _cleanup_old_records(self, days: int) -> None:
        try:
            await self.cursor.execute(
                "DELETE FROM Items WHERE timestamp < NOW() - (INTERVAL '1 day' * %s)",
                (days,),
            )
        except Exception as e:
            self.spider_logger.error(f"_cleanup_old_records: {e}")
        else:
            await self.conn.commit()

    def is_recent(
        self, date_str: str, date_format: str, debug_info: str, spider: Spider
    ) -> bool:
        """
        Check if the date is recent (within the last 2 days).
        """
        try:
            if not date_str:
                return True
            utc_today = datetime.now(ZoneInfo("UTC")).date()
            input_date = dateparser.parse(
                date_string=date_str,
                date_formats=[date_format] if date_format is not None else None,
            ).date()
            return input_date >= (utc_today - timedelta(days=2))
        except Exception as e:
            spider.logger.error(f"{str(e)}: {debug_info} ")
            return False

    def _drop_item(self, item, errors):
        self.stats.add_dropped_item()
        raise DropItem(f"Validation failed! {errors}")

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        item = {
            k: "\n".join([" ".join(line.split()) for line in v.strip().splitlines()])
            if isinstance(v, str)
            else v
            for k, v in item.items()
        }
        if self.validation_enabled and "_skip_validation" not in item:
            try:
                item = super().process_item(item, spider)
            except DropItem as e:
                raise e

        _id = item.get("_id", None)
        if _id:
            _id = normalize_url(_id)
            id_exists = await self.db_exists(_id, spider.name)
            if id_exists:
                raise DropItem(f"Already exists [{_id}]")
            else:
                await self.db_insert(_id, spider.name, item)
        _dt = item.pop("_dt", None)
        _dt_format = item.pop("_dt_format", None)
        if _dt:
            if not self.is_recent(_dt, _dt_format, item.get("_id"), spider):
                raise DropItem(f"Outdated [{_dt}]")

        if not {k: v for k, v in item.items() if not k.startswith("_")}:
            raise DropItem("item has no data fields")

        # replace 'not scraped yet' & 'n/a' with empty or null in body (just to comply with gRPC feed)
        if item.get("body") in ["not scraped yet", "n/a"]:
            item["body"] = None
        if item.get("published_at") in ["n/a"]:
            item['published_at'] = None
        return item


class PostProcessingPipeline:
    """
    Pipeline to postprocess items.
    Handles DB insertion.

    Attributes:
        settings (Settings): crawler settings object
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["DB_NAME", "DB_HOST", "DB_PORT", "DB_USER", "DB_PASS"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        p = cls(settings=crawler.settings)
        crawler.signals.connect(p.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(p.spider_closed, signal=signals.spider_closed)
        return p

    async def spider_opened(self, spider: Spider) -> None:
        try:
            self.conn = await psycopg.AsyncConnection.connect(f"""
                dbname={self.settings.get("DB_NAME")}
                user={self.settings.get("DB_USER")}
                password={self.settings.get("DB_PASS")}
                host={self.settings.get("DB_HOST")}
                port={self.settings.get("DB_PORT")}
            """)
        except:
            raise NotConfigured("Failed to connect to DB")
        self.cursor = self.conn.cursor()

    async def spider_closed(self, spider: Spider) -> None:
        if hasattr(self, "conn"):
            await self.cursor.close()
            await self.conn.close()

    async def db_remove(self, id: str, spider_name: str) -> None:
        await self.cursor.execute(
            "DELETE FROM Items WHERE id=%s AND spider=%s", (id, spider_name)
        )

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        if not item.pop("_delivered", None):
            _id = item.get("_id", None)
            if _id:
                _id = normalize_url(_id)
                await self.db_remove(_id, spider.name)
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
        return cls(uri=crawler.settings.get("DISCORD_SERVER_URI"))

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        try:
            _item = {
                k: v
                for k, v in item.items()
                if not k.startswith("_") and k.lower() not in self.exclude_fields
            }
            await maybe_deferred_to_future(
                spider.crawler.engine.download(
                    scrapy.Request(
                        url=self.uri,
                        method="POST",
                        body=json.dumps(
                            {
                                "embeds": [
                                    {
                                        "title": "Alert",
                                        "description": json.dumps(_item),
                                        "color": int("03b2f8", 16),
                                    }
                                ]
                            }
                        ),
                        headers={"Content-Type": "application/json"},
                        callback=NO_CALLBACK,
                        errback=lambda f: spider.logger.error((f.value)),
                    ),
                )
            )
            item["_delivered"] = True
        except Exception as e:
            spider.logger.error(f"Failed to send to Discord: {item['_id']}\n{str(e)}")


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
        settings = ["SYNOPTIC_SERVER_URI", "SYNOPTIC_STREAM_ID", "SYNOPTIC_API_KEY"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        return cls(
            uri=crawler.settings.get("SYNOPTIC_SERVER_URI"),
            stream_id=crawler.settings.get("SYNOPTIC_STREAM_ID"),
            api_key=crawler.settings.get("SYNOPTIC_API_KEY"),
        )

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        try:
            _item = {
                k: v
                for k, v in item.items()
                if not k.startswith("_") and k.lower() not in self.exclude_fields
            }
            await maybe_deferred_to_future(
                spider.crawler.engine.download(
                    scrapy.Request(
                        url=self.uri,
                        body=json.dumps(_item),
                        method="POST",
                        headers={
                            "content-type": "application/json",
                            "x-api-key": self.api_key,
                        },
                        callback=NO_CALLBACK,
                        errback=lambda f: spider.logger.error((f.value)),
                    )
                )
            )
            item["_delivered"] = True
        except Exception as e:
            spider.logger.error(f"Failed to send to Synoptic: {item['_id']}\n{str(e)}")


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

    def __init__(self, uri: str, token: str, chat_id: str) -> None:
        self.uri = uri
        self.token = token
        self.chat_id = chat_id

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["TELEGRAM_SERVER_URI", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
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
        try:
            _item = {
                k: v
                for k, v in item.items()
                if not k.startswith("_") and k.lower() not in self.exclude_fields
            }
            await maybe_deferred_to_future(
                spider.crawler.engine.download(
                    scrapy.Request(
                        url=self.uri,
                        body=json.dumps(_item),
                        method="POST",
                        headers={
                            "content-type": "application/json",
                            "authorization": self.token,
                        },
                        callback=NO_CALLBACK,
                        errback=lambda f: spider.logger.error((f.value)),
                    )
                )
            )
            item["_delivered"] = True
        except Exception as e:
            spider.logger.error(f"Failed to send to Telegram: {item['_id']}\n{str(e)}")


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


    def __init__(
        self, uri: str, token: str, id: str, id_headline: str, proto_module: str,
    ) -> None:
        self.uri = uri
        self.token = token
        self.id = id
        self.id_headline = id_headline
        self.feed_pb2 = importlib.import_module(f"{proto_module}.feed_pb2")
        self.feed_pb2_grpc = importlib.import_module(f"{proto_module}.feed_pb2_grpc")
        self.channel_grpc: grpc.aio.Channel = None
        self.client_grpc = None
        self.connected = asyncio.Event()
        self.t: asyncio.Task = None
        self.sem = asyncio.Semaphore(16)


    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["GRPC_SERVER_URI", "GRPC_TOKEN", "GRPC_ID", "GRPC_PROTO_MODULE"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        p = cls(
            uri=crawler.settings.get("GRPC_SERVER_URI"),
            token=crawler.settings.get("GRPC_TOKEN"),
            id=crawler.settings.get("GRPC_ID"),
            id_headline=crawler.settings.get("GRPC_ID_HEADLINE"),
            proto_module=crawler.settings.get("GRPC_PROTO_MODULE"),
        )
        crawler.signals.connect(p.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(p.spider_closed, signal=signals.spider_closed)
        return p


    async def close_connection(self) -> None:
        if self.channel_grpc:
            await self.channel_grpc.close()
        self.channel_grpc = None
        self.client_grpc = None
        self.connected.clear()


    async def spider_opened(self, spider: Spider) -> None:
        self.connected.clear()
        self.t = asyncio.create_task(self.connect(spider))


    async def spider_closed(self, spider: Spider) -> None:
        if self.t and not self.t.done():
            self.t.cancel()
            try:
                await self.t
            except asyncio.CancelledError:
                pass
        await self.close_connection()


    async def connect(self, spider: Spider) -> None:
        while True:
            if self.connected.is_set():
                await asyncio.sleep(2.0)
                continue
            spider.logger.debug("connecting to gRPC server")
            try:
                self.channel_grpc = grpc.aio.secure_channel(self.uri, grpc.ssl_channel_credentials())
                self.client_grpc = self.feed_pb2_grpc.IngressServiceStub(self.channel_grpc)
                await asyncio.wait_for(self.channel_grpc.channel_ready(), timeout=10.0)
            except (Exception, asyncio.TimeoutError) as e:
                spider.logger.error(e)
                await self.close_connection()
                continue
            else:
                spider.logger.debug("connected to gRPC server")
                self.connected.set()


    async def process_item(self, item: Dict, spider: str) -> Dict:
        async with self.sem:
            await self._send(item, spider)
        return item


    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {
            k: self.to_timestamp(v, spider) if k in ["scraped_at", "published_at"] else v
            for k, v in item.items()
            if not k.startswith("_") and k.lower() not in self.exclude_fields
        }
        feed_id = self.id
        if ("body" in _item) and (_item["body"] is None) and self.id_headline:
            feed_id = self.id_headline
        feed_message = self.feed_pb2.FeedMessage(
            token=self.token,
            feedId=feed_id,
            messageId=item['_id'],
            message=json.dumps(_item),
        )
        await self.connected.wait()
        try:
            await self.client_grpc.SubmitFeedMessage(feed_message)
        except grpc.RpcError as e:
            spider.logger.error(f"Failed to send to gRPC server: {item['_id']}\n{str(e)}")
            await self.close_connection()
        else:
            item["_delivered"] = True
            spider.logger.debug(f"Sent to gRPC server [{feed_id}]: {item['_id']}")


    def to_timestamp(self, dt: str, spider: Spider) -> int:
        if not dt:
            return None
        try:
            dt = dateparser.parse(dt)
            return int(dt.timestamp() * 1000)
        except Exception as e:
            spider.logger.error(f"Failed to convert datetime to timestamp: {str(e)}")


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
        p = cls(uri=crawler.settings.get("WS_SERVER_URI"))
        crawler.signals.connect(p.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(p.spider_closed, signal=signals.spider_closed)
        return p

    async def spider_opened(self, spider: Spider) -> None:
        self.client = await websockets.connect(self.uri)

    async def spider_closed(self, spider: Spider) -> None:
        await self.client.close()

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        _item = {
            k: v
            for k, v in item.items()
            if not k.startswith("_") and k.lower() not in self.exclude_fields
        }
        try:
            await self.client.send(json.dumps(_item))
            item["_delivered"] = True
            spider.logger.debug(f"Sent to WS server: {item['_id']}")
        except Exception as e:
            spider.logger.error(f"Failed to send to WS server: {item['_id']}\n{str(e)}")
            self.client = await websockets.connect(self.uri)


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
        settings = ["HTTP_SERVER_URI", "HTTP_TOKEN"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        p = cls(
            uri=crawler.settings.get("HTTP_SERVER_URI"),
            token=crawler.settings.get("HTTP_TOKEN"),
        )
        return p

    async def process_item(self, item: Dict, spider: Spider) -> Dict:
        await self._send(item, spider)
        return item

    async def _send(self, item: Dict, spider: Spider) -> None:
        try:
            _item = {
                k: v
                for k, v in item.items()
                if not k.startswith("_") and k.lower() not in self.exclude_fields
            }
            await maybe_deferred_to_future(
                spider.crawler.engine.download(
                    scrapy.Request(
                        url=self.uri,
                        body=json.dumps(_item),
                        method="POST",
                        headers={
                            "content-type": "application/json",
                            "authorization": self.token,
                        },
                        callback=NO_CALLBACK,
                        errback=lambda f: spider.logger.error((f.value)),
                    )
                )
            )
            item["_delivered"] = True
        except Exception as e:
            spider.logger.error(
                f"Failed to send to HttpWebhook: {item['_id']}\n{str(e)}"
            )
