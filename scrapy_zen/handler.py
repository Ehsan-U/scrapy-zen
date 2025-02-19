from scrapy_playwright.handler import ScrapyPlaywrightDownloadHandler
from scrapy_zyte_api import ScrapyZyteAPIDownloadHandler
from scrapy_impersonate import ImpersonateDownloadHandler
from scrapy.core.downloader.handlers.http import HTTPDownloadHandler
from twisted.internet.defer import Deferred
from scrapy.http import Request, Response
from scrapy import Spider
from scrapy.crawler import Crawler
from typing import Self


class ZenDownloadHandler:
    
    def __init__(self, crawler: Crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(crawler)

    def download_request(self, request: Request, spider: Spider) -> Deferred | Deferred[Response]:
        if request.meta.get('playwright'):
            if not hasattr(self, "playwright_handler"):
                self.playwright_handler = ScrapyPlaywrightDownloadHandler.from_crawler(self.crawler)
            return self.playwright_handler.download_request(request, spider)

        elif request.meta.get("impersonate"):
            if not hasattr(self, "impersonate_handler"):
                self.impersonate_handler = ImpersonateDownloadHandler.from_crawler(self.crawler)
            return self.impersonate_handler.download_request(request, spider)

        elif request.meta.get('zyte_api_automap'):
            if not hasattr(self, "zyte_handler"):
                self.zyte_handler = ScrapyZyteAPIDownloadHandler.from_crawler(self.crawler)
            return self.zyte_handler.download_request(request, spider)

        else:
            if not hasattr(self, "scrapy_default_handler"):
                self.scrapy_default_handler = HTTPDownloadHandler.from_crawler(self.crawler)
            return self.scrapy_default_handler.download_request(request, spider)
