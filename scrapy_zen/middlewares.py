from tinydb import Query, TinyDB
from scrapy import Spider, signals
from scrapy.exceptions import IgnoreRequest
from datetime import datetime
import dateparser
from scrapy.crawler import Crawler
from typing import Self



class PreProcessingMiddleware:
    """
    Pipeline to preprocess requests, whatever you wanna to with request before downloading it.
    Don't do any DB modification here !

    Attributes:
        file_path (str): Path to TinyDB database file. Defaults to "db.json"
    """

    def __init__(self, file_path: str = "db.json") -> None:
        self.file_path = file_path

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        m = cls(
            file_path=crawler.settings.get("PREPROCESSING_DB_PATH"),
        )
        crawler.signals.connect(m.open_spider, signal=signals.spider_opened)
        crawler.signals.connect(m.close_spider, signal=signals.spider_closed)
        return m

    def open_spider(self, spider: Spider) -> None:
        self._init_db()

    def close_spider(self, spider: Spider) -> None:
        if hasattr(self, "db"):
            self._db.close()
    
    def _init_db(self) -> None:
        self._db = TinyDB(self.file_path)
        self._query = Query()

    def process_request(self, request, spider: Spider) -> None:
        _id = request.meta.pop("_id", None)
        if _id:
            if self.tinydb_exists(id=_id):
                raise IgnoreRequest
        _dt = request.meta.pop("_dt", None)
        _dt_format = request.meta.pop("_dt_format", None)
        if _dt:
            if not self.is_today(_dt, _dt_format):
                raise IgnoreRequest
        return None
    
    def tinydb_exists(self, id: str) -> bool:
        return bool(self._db.search(self._query.id == id))

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