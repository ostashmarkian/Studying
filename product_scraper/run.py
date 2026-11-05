from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

from product_scraper.spiders.products import ProductsSpider

def item_name(item):
    print(f"Item scraped {item["title"]}")
def main():
    process = CrawlerProcess(get_project_settings())
    crawler = process.create_crawler(ProductsSpider)
    crawler.signals.connect(item_name,signal=signals.item_scraped)
    process.crawl(crawler)
    process.start()
    
    
if __name__ == "__main__":
    main()