import scrapy
from scrapy.crawler import CrawlerProcess
import logging
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

class Book(scrapy.Spider):
    name = "book"
    start_urls = ['httpsaa//books.toscrape.com/']

    def parse(self, response):
        for book in response.xpath("//article"):
            yield {
                "_id": book.xpath(".//h3/a/text()").get(),
                "title": book.xpath(".//h3/a/text()").get()
            }

crawler = CrawlerProcess(settings={
    "ITEM_PIPELINES": {
        "scrapy_zen.pipelines.PreProcessingPipeline": 543
    },
    "ADDONS": {"scrapy_zen.addons.SpidermonAddon": 1},
    "LOG_FILE": "logs.log",
    "LOG_FILE_APPEND": False,
})
crawler.crawl(Book)
crawler.start()

# import logparser
# from pprint import pprint

# with open("logs.log", 'r') as f:
#     logs = f.read()

# d = logparser.parse(logs)
# pprint(d['log_categories']['error_logs']['details'])