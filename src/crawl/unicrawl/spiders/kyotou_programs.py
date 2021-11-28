from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://www.z.k.kyoto-u.ac.jp'

# Note: need to change the parameter ROBOTS_OBEY in the crawler settings.py to make the crawler work


class KyotoUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Kyoto University
    """

    name = "kyotou-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}kyotou_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(url=BASE_URL + '/zenkyo/list',
                             cookies={'locale': 'ja'}, callback=self.parse_main)

    def parse_main(self, response):
        faculties = response.xpath("//h3/span/text()").getall()
        log.info(faculties)
        self.courses_ids = {}
        for faculty in faculties:
            program_links = response.xpath(
                f"//h3[span/text()='{faculty}']/following::ul[1]/li/a/@href").getall()
            program_names = response.xpath(
                f"//h3[span/text()='{faculty}']/following::ul[1]/li/a/text()").getall()
            faculty = faculty.replace("科目群", "")
            log.info(list(zip(program_links, program_names)))
            for program_link, program_name in zip(program_links, program_names):
                self.courses_ids[program_link] = []
                yield response.follow(
                    program_link, self.parse_program,
                    cb_kwargs={"faculty": faculty, "program_link": program_link,
                               "program_name": program_name})

    def parse_program(self, response, faculty, program_link, program_name):
        course_links = response.xpath("//div[@class='sp-hidden']//tr/td[@class='result-control']/div[1]/a[1]/@href").getall()
        courses_ids = list(map(lambda x: x.split("#lecture_")[1], course_links))

        self.courses_ids[program_link] += courses_ids

        next_page = response.xpath("//nav[@class='pagination']/span[@class='page current']"
                                   "/following-sibling::span[1]/a/@href").get()
        log.info(f"{next_page=}")

        if next_page is not None and len(next_page.strip()) > 0:
            yield response.follow(
                next_page, self.parse_program,
                cb_kwargs={"faculty": faculty, "program_link": program_link,
                           "program_name": program_name})
            return

        log.info(f"{program_name=} {self.courses_ids[program_link]=}")

        program_id = '_'.join([faculty, program_name])
        cycle = "grad"

        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": [faculty],
            "campuses": [],  # didn't find information on campuses
            "url": BASE_URL + program_link,
            "courses": list(set(self.courses_ids[program_link])),
            "ects": []  # ECTS not applicable in Japan
        }
