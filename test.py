import scrapy
from scrapy.crawler import CrawlerProcess
import logging
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)

class Book(scrapy.Spider):
    name = "book"
    start_urls = ['https://books.toscrape.com/']

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
})
crawler.crawl(Book)
crawler.start()
