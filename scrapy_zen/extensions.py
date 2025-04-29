from scrapy.crawler import Crawler
from scrapy.statscollectors import StatsCollector
from typing import Self
from scrapy import Spider, signals
from scrapy.http import Request, Response
from time import time



class ZenExtension:
    """
    Allows to calculate average latency across requests
    """

    def __init__(self, stats: StatsCollector) -> None:
        self.stats = stats
        self.response_count = 0
        self.total_latency = 0.0
        self.max_latency = None
        self.min_latency = None

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        ext = cls(crawler.stats)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.request_reached_downloader, signal=signals.request_reached_downloader)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext
    
    def request_reached_downloader(self, request: Request, spider: Spider) -> None:
        request.meta['zen_start_time'] = time()
    
    def response_received(self, response: Response, request: Request, spider: Spider) -> None:
        download_latency = time() - request.meta.pop("zen_start_time")
        if self.min_latency is None:
            self.min_latency = download_latency
            self.max_latency = download_latency
        else:
            if download_latency > self.max_latency:
                self.max_latency = download_latency
            if download_latency < self.min_latency:
                self.min_latency = download_latency
        self.total_latency += download_latency
        self.response_count += 1

    def spider_closed(self, spider: Spider, reason: str) -> None:
        if self.response_count > 0:
            avg_response_latency = self.total_latency / self.response_count
            self.stats.set_value("zen/avg_latency_seconds", f"{avg_response_latency:.2f}")
            self.stats.set_value("zen/min_latency_seconds", f"{self.min_latency:.2f}")
            self.stats.set_value("zen/max_latency_seconds", f"{self.max_latency:.2f}")
            self.stats.set_value("zen/response_count", self.response_count) 
