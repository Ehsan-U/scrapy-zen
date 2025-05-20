import scrapy
from scrapy.crawler import CrawlerProcess
import logging
from dotenv import load_dotenv
load_dotenv()
logging.getLogger("urllib3.connectionpool").setLevel(logging.WARNING)


class BookSpider(scrapy.Spider):
    name = 'books'
    allowed_domains = ['books.toscrape.com']

    def start_requests(self):
        url = "http://books.toscrape.com/catalogue/category/books_1/index.html"
        yield scrapy.Request(url=url, callback=self.parse, dont_filter=True)

    def parse(self, response):
        # Find all book articles on the page
        books = response.css('article.product_pod')
        
        for book in books:
            yield {
                'title': book.css('h3 a::attr(title)').get(),
                'price': book.css('p.price_color::text').get(),
                'rating': book.css('p.star-rating::attr(class)').get().split()[-1],
                'availability': book.css('p.availability::text').get().strip(),
                'url': book.css('h3 a::attr(href)').get()
            }
            yield response.follow(
                book.css('h3 a::attr(href)').get(),
                callback=self.parse_book,
                meta={'book_title': book.css('h3 a::attr(title)').get()}
            )

    def parse_book(self, response):
        book_title = response.meta['book_title']
        book_description = response.css('meta[name="description"]::attr(content)').get()
        book_description = book_description.split(' - ')[0]
        book_image_url = response.css('img::attr(src)').get()
        book_image_url = response.urljoin(book_image_url)
        
        yield {
            'book_title': book_title,
            'book_description': book_description,
            'book_image_url': book_image_url
        }


crawler = CrawlerProcess(settings={
    "DOWNLOADER_MIDDLEWARES": {
        "scrapy_zen.middlewares.PreProcessingMiddleware": 543,
    },
    "ITEM_PIPELINES": {
        "scrapy_zen.pipelines.PreProcessingPipeline": 543,
        "scrapy_zen.pipelines.PostProcessingPipeline": 544
    },
    "ADDONS": {
        "scrapy_zen.addons.ZenAddon": 1,
        "scrapy_zen.addons.SpidermonAddon": 2,
    },
    "LOG_FILE": "logs.log",
    "ZEN_VALIDATION_SCHEMA": "NEWS",
    # "LOG_FILE_APPEND": False,
    "BOT_NAME": "culture_entertainment",
})
crawler.crawl(BookSpider)
crawler.start()