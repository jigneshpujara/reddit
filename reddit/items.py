# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Field


class RedditItem(scrapy.Item):

    reddit_post = scrapy.Field()
    comment_data = scrapy.Field()
    user = scrapy.Field()
