import scrapy


class EbaySpider(scrapy.Spider):
    name = "ebay"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/"]

    def parse(self, response):

        books = response.css("article.product_pod")

        for book in books:

            yield {
                "title": book.css("h3 a::attr(title)").get(),
                "price": book.css(".price_color::text").get(),
                "availability": book.css(".availability::text").getall()[-1].strip(),
            }

        next_page = response.css("li.next a::attr(href)").get()

        if next_page:
            yield response.follow(next_page, callback=self.parse)
