from functools import wraps
from typing import Dict, Callable
from scrapy.crawler import Crawler
from scrapy.spiders import Spider
from scrapy.exceptions import DropItem
from tinydb import TinyDB, Query
from datetime import datetime, timedelta
import dateparser



class PreProcessingPipeline:
    """
    Pipeline to preprocess items before forwarding.
    Handles deduplication, date filtering, and data cleaning.

    Args:
        file_path (str): Path to TinyDB database file. Defaults to "db.json"
        expiry_days (int): Number of days to keep records. Defaults to 60
    """

    def __init__(self, file_path: str = "db.json", expiry_days: int = 60):
        self.file_path = file_path
        self.expiry_days = expiry_days

    @classmethod
    def from_crawler(cls, crawler: Crawler):
        return cls(
            file_path=crawler.settings.get("PREPROCESSING_DB_PATH"),
            expiry_days=crawler.settings.get("PREPROCESSING_EXPIRY_DAYS"),
        )

    def open_spider(self, spider: Spider):
        self._init_db()

    def close_spider(self, spider: Spider):
        if hasattr(self, "db"):
            self._db.close()

    def _init_db(self):
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
    def process_item(self, item: Dict, spider: Spider):
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