import scrapy
import re


class WikiSpider(scrapy.Spider):
    name = "wiki"
    allowed_domains = ["ru.wikipedia.org"]
    start_urls = ['https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту']

    # НАСТРОЙКА ОГРАНИЧЕНИЯ
    pages_limit = 10 # Число страниц или All (все страницы)
    pages_processed = 1  

    def parse(self, response):
        movie_links = response.css('#mw-pages .mw-category-group a::attr(href)').getall()
        for link in movie_links:
            yield response.follow(link, callback=self.parse_movie, priority=10)

        next_page = response.xpath("//a[contains(text(), 'Следующая страница')]/@href").get()

        should_continue = (
                self.pages_limit == 'All' or
                self.pages_processed < self.pages_limit
        )

        if next_page and should_continue:
            self.pages_processed += 1
            self.logger.info(f"--- ПЕРЕХОД {self.pages_processed} ИЗ {self.pages_limit} ---")
            yield response.follow(next_page, callback=self.parse, dont_filter=True, priority=1)
        else:
            self.logger.info("--- ЛИМИТ СТРАНИЦ ДОСТИГНУТ ИЛИ СЛЕДУЮЩЕЙ СТРАНИЦЫ НЕТ. ОСТАНОВКА. ---")

    def parse_movie(self, response):
        def get_clean_text(label):
            nodes = response.xpath(
                f"//table[contains(@class, 'infobox')]//th[contains(., '{label}')]/following-sibling::td//text()[not(parent::style) and not(parent::sup)]"
            ).getall()
            return [n.strip() for n in nodes if n.strip() and n.strip() not in [',', '[', ']']]

        title = response.css('th.infobox-above::text').get()
        if not title or title.strip() in ["?", "???", ""]:
            title = response.css('h1#firstHeading i::text').get() or response.css('h1#firstHeading::text').get()

        year_data = "".join(get_clean_text('Год') + get_clean_text('Выпуск') + get_clean_text('Дата выхода'))
        year_match = re.search(r'\d{4}', year_data)
        year = year_match.group(0) if year_match else None

        yield {
            'Название': title.strip() if title else "Без названия",
            'Жанр': ", ".join(get_clean_text('Жанр')),
            'Режиссер': ", ".join(get_clean_text('Режиссёр')),
            'Страна': ", ".join(get_clean_text('Страна')),
            'Год': year

        }
