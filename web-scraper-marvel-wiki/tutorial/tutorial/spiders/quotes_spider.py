from pathlib import Path

import scrapy
from scrapy.shell import inspect_response


class QuotesSpider(scrapy.Spider):
    name = "quotes"
    root_url = "https://marvel.fandom.com"

    def start_requests(self):

        yield scrapy.Request(self.root_url + '/wiki/Category:Characters?from=A', callback=self.parseAllPages)

        #for url in urls:
        #    yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        page = response.url.split("/")[-1]
        filename = f"./wikipages/{page}.html"
        Path(filename).write_bytes(response.body)
        self.log(f"Saved file {filename}")

    def parseAllPages(self, response):
        ul = response.css('div.category-page__members')[0]
        allLinks = ul.css('a.category-page__member-link::attr(href)')

        for linkObject in allLinks:
            link = linkObject.get()
            if link is not None:
                yield scrapy.Request(self.root_url + link, callback=self.parse)

        navdiv = response.css('div.category-page__pagination')

        for link_object in navdiv.css('a.category-page__pagination-next'):
            link_url = link_object.css('::attr(href)').get()

            if link_url is not None:
                self.log
                yield scrapy.Request(link_url, callback=self.parseAllPages)