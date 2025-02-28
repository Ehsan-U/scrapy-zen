import logging
import os
from typing import List, Self, Dict, Any
from scrapy import logformatter
from scrapy.crawler import Crawler
from scrapy.exceptions import DropItem
from scrapy import Spider
from scrapy.http import Response
from scrapy.utils.python import global_object_name
from twisted.python.failure import Failure
from scrapy.logformatter import LogFormatterResult



class ZenLogFormatter(logformatter.LogFormatter):
    YELLOW = "\033[33m"
    RED = "\033[31m"
    RESET = "\033[0m"
    default_truncate_events: List[str] = ["dropped","item_error"]
    
    def __init__(self, truncate_fields: List[str], truncate_events: List[str]) -> None:
        self.truncate_fields = truncate_fields
        self.truncate_events = self.default_truncate_events + truncate_events

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(
            truncate_fields=crawler.settings.getlist("FORMATTER_TRUNCATE_FIELDS", []),
            truncate_events=crawler.settings.get("FORMATTER_TRUNCATE_EVENTS", []),
        )
    
    @staticmethod
    def truncate(value: Any, length=50) -> Any:
        if isinstance(value, str):
            return value[:length] + '...' if len(value) > length else value
        return value

    def dropped(self, item: Dict, exception: DropItem, response: Response, spider: Spider) -> LogFormatterResult:
        if "dropped" in self.truncate_events:
            return {
                'level': logging.WARNING,
                'msg': self.YELLOW + "Dropped: %(exception)s" + self.RESET + os.linesep + "%(item)s",
                'args': {
                    'exception': exception,
                    'item': {k:self.truncate(v) if k in self.truncate_fields else v for k,v in item.items()},
                }
            }
        else:
            return super().dropped(item, exception, response, spider)
    
    def item_error(self, item: Dict, exception: DropItem, response: Response, spider: Spider) -> LogFormatterResult:
        if "item_error" in self.truncate_events:
            return {
                'level': logging.ERROR,
                'msg': self.RED + "Error processing %(item)s" + self.RESET,
                'args': {
                    'exception': exception,
                    'item': {k:self.truncate(v) if k in self.truncate_fields else v for k,v in item.items()},
                }
            }
        else:
            return super().item_error(item, exception, response, spider)
    
    def scraped(self, item: Dict, response: Response, spider: Spider) -> LogFormatterResult:
        if "scraped" in self.truncate_events:
            src: Any
            if response is None:
                src = f"{global_object_name(spider.__class__)}.start_requests"
            elif isinstance(response, Failure):
                src = response.getErrorMessage()
            else:
                src = response
            return {
                "level": logging.DEBUG,
                "msg": "Scraped from %(src)s" + os.linesep + "%(item)s",
                "args": {
                    "src": src,
                    "item": {k:self.truncate(v) if k in self.truncate_fields else v for k,v in item.items()},
                },
            }
        else:
            return super().scraped(item, response, spider)
    