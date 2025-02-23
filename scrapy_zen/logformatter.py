import logging
import os
from typing import List, Self, Dict, Any
from scrapy import logformatter
from scrapy.crawler import Crawler
from scrapy.exceptions import DropItem
from scrapy import Spider
from scrapy.http import Response


class ZenLogFormatter(logformatter.LogFormatter):
    
    def __init__(self, truncate_fields: List[str]) -> None:
        self.truncate_fields = truncate_fields

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(
            truncate_fields=crawler.settings.getlist("FORMATTER_TRUNCATE_FIELDS", [])
        )
    
    @staticmethod
    def truncate(value: Any, length=50) -> Any:
        if isinstance(value, str):
            return value[:length] + '...' if len(value) > length else value
        return value

    def dropped(self, item: Dict, exception: DropItem, response: Response, spider: Spider) -> Dict:
        return {
            'level': logging.WARNING,
            'msg': "Dropped: %(exception)s" + os.linesep + "%(item)s",
            'args': {
                'exception': exception,
                'item': {k:self.truncate(v) if k in self.truncate_fields else v for k,v in item.items()},
            }
        }
    
    def item_error(self, item: Dict, exception: DropItem, response: Response, spider: Spider) -> Dict:
        return {
            'level': logging.ERROR,
            'msg': "Error processing %(item)s",
            'args': {
                'exception': exception,
                'item': {k:self.truncate(v) if k in self.truncate_fields else v for k,v in item.items()},
            }
        }
    