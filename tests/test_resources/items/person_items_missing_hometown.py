import scrapy
from scrapy import Field


class NameItem(scrapy.Item):
    name = Field()
    saved_at = Field()


class PersonItem(scrapy.Item):
    name = Field()    
    height = Field()
    weight = Field()
    shirt_color = Field()