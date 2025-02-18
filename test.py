import scrapy
from scrapy.crawler import CrawlerProcess


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
    "ADDONS": {"scrapy_zen.addon.SpidermonAddon": 1},
    "DB_NAME": "mydb",
    "DB_HOST": "127.0.0.1",
    "DB_PORT": "5432",
    "DB_USER": "root",
    "DB_PASS": "toor"
})
crawler.crawl(Book)
crawler.start()
