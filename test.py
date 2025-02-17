import scrapy
from scrapy.crawler import CrawlerProcess


class Book(scrapy.Spider):
    name = "book"
    start_urls = ['https://books.toscrape.com/']

    def parse(self, response):
        for book in response.xpath("//article"):
            yield {
                "title": book.xpath(".//h3/a/text()").get()
            }

crawler = CrawlerProcess(settings={
    "PREPROCESSING_DB_PATH": "db.json",
    "ITEM_PIPELINES": {
        "scrapy_zen.pipelines.PreProcessingPipeline": 543
    }
})
crawler.crawl(Book)
crawler.start()