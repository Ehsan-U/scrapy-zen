from zoneinfo import ZoneInfo
from scrapy import Spider, signals
from scrapy.exceptions import IgnoreRequest, NotConfigured
from datetime import datetime
import dateparser
from scrapy.crawler import Crawler
from typing import Self
from scrapy.settings import Settings
import psycopg



class PreProcessingMiddleware:
    """
    Middleware to preprocess requests before forwarding.
    Handles deduplication

    Attributes:
        settings (Settings): crawler settings object
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        settings = ["DB_NAME","DB_HOST","DB_PORT","DB_USER","DB_PASS"]
        for setting in settings:
            if not crawler.settings.get(setting):
                raise NotConfigured(f"{setting} is not set")
        m = cls(
            settings=crawler.settings 
        )
        crawler.signals.connect(m.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(m.close_spider, signal=signals.spider_closed)
        return m

    def open_spider(self, spider: Spider) -> None:
        try:
            self._conn = psycopg.Connection.connect(f"""
                dbname={self.settings.get("DB_NAME")} 
                user={self.settings.get("DB_USER")} 
                password={self.settings.get("DB_PASS")} 
                host={self.settings.get("DB_HOST")} 
                port={self.settings.get("DB_PORT")}
            """)
        except:
            raise NotConfigured("Failed to connect to DB")
        self._cursor = self._conn.cursor()

    def close_spider(self, spider: Spider) -> None:
        if hasattr(self, "_conn"):
            self._conn.close()
    
    def db_exists(self, id: str) -> bool:
        record = self._cursor.execute("SELECT id FROM Items WHERE id = %s", (id,)).fetchone()
        return bool(record)

    def process_request(self, request, spider: Spider) -> None:
        _id = request.meta.pop("_id", None)
        if _id:
            if self.db_exists(id=_id):
                raise IgnoreRequest
        _dt = request.meta.pop("_dt", None)
        _dt_format = request.meta.pop("_dt_format", None)
        if _dt:
            if not self.is_today(_dt, _dt_format, request.url, spider):
                raise IgnoreRequest
        return None
    
    def is_today(self, date_str: str, date_format: str, debug_info: str, spider: Spider) -> bool:
        try:
            if not date_str:
                return True
            today = datetime.now(ZoneInfo('America/New_York')).date()
            input_date = dateparser.parse(date_string=date_str, date_formats=[date_format] if date_format is not None else None).date()
            return input_date >= today
        except Exception as e:
            spider.logger.error(f"{str(e)}: {date_str} {debug_info} ")
            return False
