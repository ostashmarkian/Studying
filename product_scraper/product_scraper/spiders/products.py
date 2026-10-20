import scrapy


class ProductsSpider(scrapy.Spider):
    name = "products"
    allowed_domains = ["sandbox.oxylabs.io"]
    start_urls = [f"https://sandbox.oxylabs.io/products?page={page}" for page in range(4)]

    def parse(self, response):
        for card in response.css("div.product-card"):
            yield {
                "title": card.css("h4.title::text").get(),
                "price": card.css("div.price-wrapper::text").get()
            }
