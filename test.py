import scrapy
from scrapy.crawler import CrawlerProcess


class Book(scrapy.Spider):
    name = "book"
    start_urls = ['https://www.tssemasek.com.sg/en/news-and-resources']

    def parse(self, response):
        for book in response.xpath("//article"):
            yield {
                "title": book.xpath(".//h3/a/text()").get()
            }

crawler = CrawlerProcess(settings={
    "PREPROCESSING_DB_PATH": "db.json",
    "ITEM_PIPELINES": {
        "scrapy_zen.pipelines.PreProcessingPipeline": 543
    },
    "SPIDERMON_ENABLED": True,
    "EXTENSIONS": {
        'spidermon.contrib.scrapy.extensions.Spidermon': 500,
    },
    "SPIDERMON_SPIDER_CLOSE_MONITORS": {
        "scrapy_zen.monitors.SpiderCloseMonitorSuite": 543
    },
    "SPIDERMON_TELEGRAM_SENDER_TOKEN": "",
    "SPIDERMON_TELEGRAM_RECIPIENTS": ["-1002462968579"],
    "SPIDERMON_MAX_CRITICALS": 0,
    "SPIDERMON_MAX_DOWNLOADER_EXCEPTIONS": 0,
    "SPIDERMON_MAX_ERRORS": 0,
    "SPIDERMON_UNWANTED_HTTP_CODES": {
        403: 0,
        429: 0,
    },
    "SPIDERMON_TELEGRAM_MESSAGE_TEMPLATE": "scrapy_zen/message.jinja",
    # "SPIDERMON_TELEGRAM_FAKE": True,
    "SPIDERMON_TELEGRAM_NOTIFIER_INCLUDE_ERROR_MESSAGES": True,
})
crawler.crawl(Book)
crawler.start()
