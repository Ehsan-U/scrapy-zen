from scrapy.settings import Settings
import os
from pkg_resources import resource_filename
from pathlib import Path
from scrapy.crawler import Crawler

from .logformatter import ZenLogFormatter


class SpidermonAddon:
    def update_settings(self, settings: Settings) -> None:
        settings.set("SPIDERMON_ENABLED", True, "addon")
        settings["EXTENSIONS"].update(
            {
                "spidermon.contrib.scrapy.extensions.Spidermon": 500,
            }
        )
        settings.set(
            "SPIDERMON_SPIDER_CLOSE_MONITORS",
            {"scrapy_zen.monitors.SpiderCloseMonitorSuite": 543},
            "addon",
        )
        settings.set("SPIDERMON_MAX_CRITICALS", 0, "addon")
        settings.set("SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS", 0, "addon")
        settings.set("SPIDERMON_MAX_ERRORS", 0, "addon")
        settings.set(
            "SPIDERMON_UNWANTED_HTTP_CODES",
            {
                401: 0,
                403: 0,
                429: 0,
                400: 0,
            },
            "addon",
        )
        # item validation
        validation_schema = settings.get("ZEN_VALIDATION_SCHEMA")
        if validation_schema:
            if validation_schema == "NEWS":
                settings.set(
                    "SPIDERMON_VALIDATION_SCHEMAS",
                    [
                        str(
                            Path(resource_filename("scrapy_zen", "schemas"))
                            / "news.json"
                        )
                    ],
                    "addon",
                )
            else:
                settings.set(
                    "SPIDERMON_VALIDATION_SCHEMAS", [validation_schema], "addon"
                )
            settings.set("SPIDERMON_VALIDATION_ADD_ERRORS_TO_ITEMS", True, "addon")
            settings.set("SPIDERMON_VALIDATION_DROP_ITEMS_WITH_ERRORS", True, "addon")
        # telegram (disabled)
        settings.set(
            "SPIDERMON_TELEGRAM_SENDER_TOKEN",
            os.getenv("SPIDERMON_TELEGRAM_SENDER_TOKEN"),
            "addon",
        )
        settings.set("SPIDERMON_TELEGRAM_RECIPIENTS", ["-1002462968579"], "addon")
        settings.set(
            "SPIDERMON_TELEGRAM_NOTIFIER_INCLUDE_ERROR_MESSAGES", True, "addon"
        )
        # discord
        settings.set(
            "SPIDERMON_DISCORD_WEBHOOK_URL",
            os.getenv("SPIDERMON_DISCORD_WEBHOOK_URL"),
            "addon",
        )


class ZenAddon:

    def __init__(self, crawler: Crawler) -> None:
        super().__init__()
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def update_settings(self, settings: Settings) -> None:
        # logger
        settings.set("LOG_FORMATTER", ZenLogFormatter, "addon")

        # Database
        settings.set("DB_NAME", os.getenv("DB_NAME"), "addon")
        settings.set("DB_USER", os.getenv("DB_USER"), "addon")
        settings.set("DB_PASS", os.getenv("DB_PASS"), "addon")
        settings.set("DB_HOST", os.getenv("DB_HOST"), "addon")
        settings.set("DB_PORT", os.getenv("DB_PORT"), "addon")

        # discord
        settings.set("DISCORD_SERVER_URI", os.getenv("DISCORD_SERVER_URI"), "addon")

        # synoptic
        settings.set("SYNOPTIC_SERVER_URI", os.getenv("SYNOPTIC_SERVER_URI"), "addon")
        settings.set("SYNOPTIC_STREAM_ID", os.getenv("SYNOPTIC_STREAM_ID"), "addon")
        settings.set("SYNOPTIC_API_KEY", os.getenv("SYNOPTIC_API_KEY"), "addon")

        # telegram
        settings.set("TELEGRAM_SERVER_URI", os.getenv("TELEGRAM_SERVER_URI"), "addon")
        settings.set("TELEGRAM_TOKEN", os.getenv("TELEGRAM_TOKEN"), "addon")
        settings.set("TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID"), "addon")

        # gRPC
        settings.set("GRPC_SERVER_URI", os.getenv("GRPC_SERVER_URI"), "addon")
        settings.set("GRPC_TOKEN", os.getenv("GRPC_TOKEN"), "addon")
        settings.set("GRPC_ID", os.getenv("GRPC_ID"), "addon")
        settings.set("GRPC_ID_HEADLINE", os.getenv("GRPC_ID_HEADLINE"), "addon")
        settings.set("GRPC_PROTO_MODULE", os.getenv("GRPC_PROTO_MODULE"), "addon")

        # websocket
        settings.set("WS_SERVER_URI", os.getenv("WS_SERVER_URI"), "addon")

        # custom http webhook
        settings.set("HTTP_SERVER_URI", os.getenv("HTTP_SERVER_URI"), "addon")
        settings.set("HTTP_TOKEN", os.getenv("HTTP_TOKEN"), "addon")

        # scrapy-zyte-api
        settings.set("ZYTE_API_KEY", os.getenv("ZYTE_API_KEY"), "addon")
        settings["DOWNLOADER_MIDDLEWARES"].update(
            {"scrapy_zyte_api.ScrapyZyteAPIDownloaderMiddleware": 643}
        )
        settings["SPIDER_MIDDLEWARES"].update(
            {
                "scrapy_zyte_api.ScrapyZyteAPISpiderMiddleware": 100,
                "scrapy_zyte_api.ScrapyZyteAPIRefererSpiderMiddleware": 1000,
            }
        )
        settings.set(
            "REQUEST_FINGERPRINTER_CLASS",
            "scrapy_zyte_api.ScrapyZyteAPIRequestFingerprinter",
            "addon",
        )

        # scrapy-playwright
        settings.set(
            "PLAYWRIGHT_ABORT_REQUEST",
            lambda req: req.resource_type == "image" or ".jpg" in req.url,
            "addon",
        )
        settings.set("PLAYWRIGHT_PROCESS_REQUEST_HEADERS", None, "addon")

        # download handler
        settings.set("DOWNLOAD_HANDLERS", {
            "http": "scrapy_zen.handler.ZenDownloadHandler",
            "https": "scrapy_zen.handler.ZenDownloadHandler",
        }, "addon")

        settings.set("USER_AGENT", None, "addon")
        settings.set("COOKIES_ENABLED", False, "addon")
        settings.set(
            "TWISTED_REACTOR",
            "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
            "addon",
        )

        # dupefilter
        jobdir = Path(resource_filename("scrapy_zen", "jobdirs")) / self.crawler.spider.name
        settings.set("ZEN_JOBDIR", str(jobdir), "addon")
        settings.set(
            "DUPEFILTER_CLASS",
            "scrapy_zen.dupefilters.ZenDupeFilter",
            "addon",
        )
