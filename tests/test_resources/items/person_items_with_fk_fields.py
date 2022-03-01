import scrapy
from scrapy import Field


class NameItem(scrapy.Item):
    name = Field()
    saved_at = Field()


class HometownItem(scrapy.Item):
    name = Field()
    population = Field()


class PersonItem(scrapy.Item):
    name_id = Field()
    hometown_id = Field()
    height = Field()
    weight = Field()
    shirt_color = Field()