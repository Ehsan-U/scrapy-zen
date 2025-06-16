from scrapy.crawler import Crawler
from scrapy.statscollectors import StatsCollector
from typing import Self
from time import time
import logging
from scrapy import Request, Spider, signals
from scrapy.exceptions import NotConfigured
from scrapy.http import Request, Response
from scrapy.core.downloader import Slot
from twisted.internet import task


logger = logging.getLogger(__name__)



class ZenExtension:
    """
    Allows to calculate average latency across requests and shows logstats
    """

    def __init__(self, stats: StatsCollector) -> None:
        self.stats = stats
        self.response_count = 0
        self.total_latency = 0.0
        self.max_latency = None
        self.min_latency = None
        self.interval: float = 60.0
        self.multiplier: float = 60.0 / self.interval
        self.task: task.LoopingCall | None = None

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        ext = cls(crawler.stats)
        crawler.signals.connect(ext.response_received, signal=signals.response_received)
        crawler.signals.connect(ext.request_reached_downloader, signal=signals.request_reached_downloader)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider: Spider) -> None:
        self.pagesprev: int = 0
        self.itemsprev: int = 0
        self.task = task.LoopingCall(self.log, spider)
        self.task.start(self.interval)

    def log(self, spider: Spider) -> None:
        self.calculate_stats()
        msg = (
            "Crawled %(pages)d pages (at %(pagerate)d pages/min), "
            "scraped %(items)d items (at %(itemrate)d items/min) [%(spider_name)s]"
        )
        log_args = {
            "pages": self.pages,
            "pagerate": self.prate,
            "items": self.items,
            "itemrate": self.irate,
            "spider_name": spider.name,
        }
        logger.info(msg, log_args, extra={"spider": spider})

    def calculate_stats(self) -> None:
        self.items: int = self.stats.get_value("item_scraped_count", 0)
        self.pages: int = self.stats.get_value("response_received_count", 0)
        self.irate: float = (self.items - self.itemsprev) * self.multiplier
        self.prate: float = (self.pages - self.pagesprev) * self.multiplier
        self.pagesprev, self.itemsprev = self.pages, self.items

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
        if self.task and self.task.running:
            self.task.stop()

        rpm_final, ipm_final = self.calculate_final_stats(spider)
        self.stats.set_value("responses_per_minute", rpm_final)
        self.stats.set_value("items_per_minute", ipm_final)

    def calculate_final_stats(
        self, spider: Spider
    ) -> tuple[None, None] | tuple[float, float]:
        start_time = self.stats.get_value("start_time")
        finish_time = self.stats.get_value("finish_time")

        if not start_time or not finish_time:
            return None, None

        mins_elapsed = (finish_time - start_time).seconds / 60

        if mins_elapsed == 0:
            return None, None

        items = self.stats.get_value("item_scraped_count", 0)
        pages = self.stats.get_value("response_received_count", 0)

        return (pages / mins_elapsed), (items / mins_elapsed)


class ZenAutoThrottle:
    def __init__(self, crawler: Crawler):
        self.crawler: Crawler = crawler
        if not crawler.settings.getbool("ZEN_AUTOTHROTTLE_ENABLED"):
            raise NotConfigured

        self.inc_factor: float = crawler.settings.getfloat(
            "ZEN_AUTOTHROTTLE_BACKOFF", 1.5
        )
        self.debug: bool = crawler.settings.getbool("ZEN_AUTOTHROTTLE_DEBUG")
        self.target_concurrency: float = crawler.settings.getfloat(
            "ZEN_AUTOTHROTTLE_TARGET_CONCURRENCY"
        )
        if self.target_concurrency <= 0.0:
            raise NotConfigured(
                f"ZEN_AUTOTHROTTLE_TARGET_CONCURRENCY "
                f"({self.target_concurrency!r}) must be higher than 0."
            )
        crawler.signals.connect(self._spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(
            self._response_downloaded, signal=signals.response_downloaded
        )

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        return cls(crawler)

    def _spider_opened(self, spider: Spider) -> None:
        self.mindelay = self._min_delay(spider)
        self.maxdelay = self._max_delay(spider)
        spider.download_delay = self._start_delay(spider)  # type: ignore[attr-defined]

    def _min_delay(self, spider: Spider) -> float:
        s = self.crawler.settings
        return getattr(spider, "download_delay", s.getfloat("DOWNLOAD_DELAY"))

    def _max_delay(self, spider: Spider) -> float:
        return self.crawler.settings.getfloat("ZEN_AUTOTHROTTLE_MAX_DELAY")

    def _start_delay(self, spider: Spider) -> float:
        return max(
            self.mindelay, self.crawler.settings.getfloat("ZEN_AUTOTHROTTLE_START_DELAY")
        )

    def _response_downloaded(
        self, response: Response, request: Request, spider: Spider
    ) -> None:
        key, slot = self._get_slot(request, spider)
        latency = request.meta.get("download_latency")
        if (
            latency is None
            or slot is None
            or request.meta.get("autothrottle_dont_adjust_delay", False) is True
        ):
            return

        olddelay = slot.delay
        self._adjust_delay(slot, latency, response)
        if self.debug:
            diff = slot.delay - olddelay
            size = len(response.body)
            conc = len(slot.transferring)
            logger.info(
                "slot: %(slot)s | conc:%(concurrency)2d | "
                "delay:%(delay)5d ms (%(delaydiff)+d) | "
                "latency:%(latency)5d ms | size:%(size)6d bytes",
                {
                    "slot": key,
                    "concurrency": conc,
                    "delay": slot.delay * 1000,
                    "delaydiff": diff * 1000,
                    "latency": latency * 1000,
                    "size": size,
                },
                extra={"spider": spider},
            )

    def _get_slot(
        self, request: Request, spider: Spider
    ) -> tuple[str | None, Slot | None]:
        key: str | None = request.meta.get("download_slot")
        if key is None:
            return None, None
        assert self.crawler.engine
        return key, self.crawler.engine.downloader.slots.get(key)

    def _adjust_delay(self, slot: Slot, latency: float, response: Response) -> None:
        """Define delay adjustment policy"""

        # If a server needs `latency` seconds to respond then
        # we should send a request each `latency/N` seconds
        # to have N requests processed in parallel
        target_delay = latency / self.target_concurrency
        current_delay = slot.delay

        if response.status == 429:
            new_delay = current_delay * self.inc_factor
        else:
            # Adjust the delay to make it closer to target_delay
            new_delay = (slot.delay + target_delay) / 2.0

            # If target delay is bigger than old delay, then use it instead of mean.
            # It works better with problematic sites.
            new_delay = max(target_delay, new_delay)

            # Make sure self.mindelay <= new_delay <= self.max_delay
            new_delay = min(max(self.mindelay, new_delay), self.maxdelay)

            # Dont adjust delay if response status != 200 and new delay is smaller
            # than old one, as error pages (and redirections) are usually small and
            # so tend to reduce latency, thus provoking a positive feedback by
            # reducing delay instead of increase.
            if response.status != 200 and new_delay <= slot.delay:
                return

        slot.delay = new_delay
