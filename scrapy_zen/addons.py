from scrapy.settings import Settings
import os
from dotenv import load_dotenv
load_dotenv()


class SpidermonAddon:

    def update_settings(self, settings: Settings) -> None:
        settings.set("SPIDERMON_ENABLED", True, "addon")
        settings['EXTENSIONS'].update({
            'spidermon.contrib.scrapy.extensions.Spidermon': 500,
        })
        settings.set('SPIDERMON_SPIDER_CLOSE_MONITORS',{
            "scrapy_zen.monitors.SpiderCloseMonitorSuite": 543
        }, "addon")
        settings.set("SPIDERMON_MAX_CRITICALS", 0, "addon")
        settings.set("SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS", 0, "addon")
        settings.set("SPIDERMON_MAX_ERRORS", 0, "addon")
        settings.set("SPIDERMON_UNWANTED_HTTP_CODES", {
            403: 0,
            429: 0,
        }, "addon")
        # telegram
        settings.set("SPIDERMON_TELEGRAM_FAKE", True, "addon")
        settings.set("SPIDERMON_TELEGRAM_SENDER_TOKEN", os.getenv("SPIDERMON_TELEGRAM_SENDER_TOKEN"), "addon")
        settings.set("SPIDERMON_TELEGRAM_RECIPIENTS", ["-1002462968579"], "addon")
        settings.set('SPIDERMON_TELEGRAM_NOTIFIER_INCLUDE_ERROR_MESSAGES', True, "addon")
        # discord
        settings.set("SPIDERMON_DISCORD_WEBHOOK_URL", os.getenv("SPIDERMON_DISCORD_WEBHOOK_URL"), "addon")


class ZenAddon:

    def update_settings(self, settings: Settings) -> None:
        # Database
        settings.set("DB_NAME", os.getenv("DB_NAME"), "addon")
        settings.set("DB_USER", os.getenv("DB_USER"), "addon")
        settings.set("DB_PASS", os.getenv("DB_PASS"), "addon")
        settings.set("DB_HOST", os.getenv("DB_HOST"), "addon")
        settings.set("DB_PORT", os.getenv("DB_PORT"), "addon")

        # discord
        settings.set("DISCORD_WEBHOOK_URI", os.getenv("DISCORD_WEBHOOK_URI"), "addon")

        # synoptic
        settings.set("SYNOPTIC_STREAM_ID", os.getenv("SYNOPTIC_STREAM_ID"), "addon")
        settings.set("SYNOPTIC_API_KEY", os.getenv("SYNOPTIC_API_KEY"), "addon")
        
        # telegram
        settings.set("TELEGRAM_TOKEN", os.getenv("TELEGRAM_TOKEN"), "addon")
        settings.set("TELEGRAM_CHAT_ID", os.getenv("TELEGRAM_CHAT_ID"), "addon")
        settings.set("TELEGRAM_WEBHOOK_URI", os.getenv("TELEGRAM_WEBHOOK_URI"), "addon")
        
        # gRPC
        settings.set("GRPC_SERVER_ADDRESS", os.getenv("GRPC_SERVER_ADDRESS"), "addon")
        settings.set("GRPC_TOKEN", os.getenv("GRPC_TOKEN"), "addon")
        settings.set("GRPC_ID", os.getenv("GRPC_ID"), "addon")
        
        # websocket
        settings.set("WS_SERVER_URI", os.getenv("WS_SERVER_URI"), "addon")

        # custom http webhook
        settings.set("HTTP_SERVER_URI", os.getenv("HTTP_SERVER_URI"), "addon")
        
        # scrapy-playwright
        settings.set("PLAYWRIGHT_ABORT_REQUEST", lambda req: req.resource_type == "image" or ".jpg" in req.url, "addon")
        settings.set("PLAYWRIGHT_PROCESS_REQUEST_HEADERS", None, "addon")
