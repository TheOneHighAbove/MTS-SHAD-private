# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import csv

class ScrapingPipeline:
    def open_spider(self, spider):
        self.file = open('movies2.csv', 'w', newline='', encoding='utf-8-sig')
        # Оставляем только нужные поля
        fieldnames = ['Название', 'Жанр', 'Режиссер', 'Страна', 'Год']
        self.writer = csv.DictWriter(self.file, fieldnames=fieldnames, delimiter=';')
        self.writer.writeheader()

    def process_item(self, item, spider):
        self.writer.writerow(item)
        return item

    def close_spider(self, spider):
        self.file.close()