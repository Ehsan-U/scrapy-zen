from zoneinfo import ZoneInfo
from scrapy import Spider, signals
from scrapy.exceptions import IgnoreRequest, NotConfigured
from datetime import datetime, timedelta
import dateparser
from scrapy.crawler import Crawler
from typing import Self
from scrapy.settings import Settings
import psycopg

from scrapy_zen import normalize_url



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
    
    def db_exists(self, id: str, spider_name: str) -> bool:
        record = self._cursor.execute("SELECT id FROM Items WHERE id=%s AND spider=%s", (id,spider_name)).fetchone()
        return bool(record)

    def process_request(self, request, spider: Spider) -> None:
        _id = request.meta.pop("_id", None)
        if _id:
            _id = normalize_url(_id)
            if self.db_exists(_id, spider.name):
                raise IgnoreRequest
        _dt = request.meta.pop("_dt", None)
        _dt_format = request.meta.pop("_dt_format", None)
        if _dt:
            if not self.is_recent(_dt, _dt_format, request.url, spider):
                raise IgnoreRequest
        return None
    
    def is_recent(self, date_str: str, date_format: str, debug_info: str, spider: Spider) -> bool:
        """
        Check if the date is recent (within the last 2 days).
        """
        try:
            if not date_str:
                return True
            utc_today = datetime.now(ZoneInfo('UTC')).date()
            input_date = dateparser.parse(date_string=date_str, date_formats=[date_format] if date_format is not None else None).date()
            return input_date >= (utc_today - timedelta(days=2))
        except Exception as e:
            spider.logger.error(f"{str(e)}: {debug_info} ")
            return False
