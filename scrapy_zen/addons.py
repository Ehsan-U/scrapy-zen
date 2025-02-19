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
        # pass it via project settings
        settings.set("SPIDERMON_TELEGRAM_SENDER_TOKEN", os.getenv("SPIDERMON_TELEGRAM_SENDER_TOKEN"), "addon")
        settings.set("SPIDERMON_TELEGRAM_RECIPIENTS", ["-1002462968579"], "addon")
        settings.set("SPIDERMON_MAX_CRITICALS", 0, "addon")
        settings.set("SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS", 0, "addon")
        settings.set("SPIDERMON_MAX_ERRORS", 0, "addon")
        settings.set("SPIDERMON_UNWANTED_HTTP_CODES", {
            403: 0,
            429: 0,
        }, "addon")
        settings.set("SPIDERMON_TELEGRAM_FAKE", True, "addon")
        settings.set('SPIDERMON_TELEGRAM_NOTIFIER_INCLUDE_ERROR_MESSAGES', True, "addon")

    
