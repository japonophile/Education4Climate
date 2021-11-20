from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'http://www.ocw.titech.ac.jp/'

# Note: need to change the parameter ROBOTS_OBEY in the crawler settings.py to make the crawler work


class TITechProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for TITech
    """

    name = "titech-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}titech_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(url=BASE_URL, callback=self.parse_main)

    def parse_main(self, response):

        faculties = response.xpath("//ul[@id='top-mein-navi']//a/@href").getall()
        # log.info(faculties)
        for faculty in faculties:
            yield response.follow(faculty, self.parse_faculty)

    def parse_faculty(self, response):
        faculty = response.xpath("//div[@id='left-menu']//li[contains(@class, 'selected')]/a/text()").getall()
        faculty = faculty[0] if len(faculty) > 0 else faculty
        if not '学院' in faculty:
            return
        # log.info(faculty)
        programs = response.xpath("//div[@id='left-menu']//li[contains(@class, 'selected')]//a/@href").getall()
        programs = [p for p in programs if p != '#']
        for program in programs:
            yield response.follow(program, self.parse_program)

    @staticmethod
    def parse_program(response):

        # log.info(f"url: {response.url}")
        faculty_id = response.url.split("GakubuCD=")[1].split("&")[0]
        if "GakkaCD" in response.url:
            main_program_id = response.url.split("GakkaCD=")[1].split("&")[0]
        else:
            main_program_id = response.url.split("KamokuCD=")[1].split("&")[0]
        sub_program_id = response.url.split("KeiCD=")[1].split("&")[0] if "KeiCD" in response.url else ""

        program_id = '_'.join([faculty_id, main_program_id, sub_program_id])
        log.info(f"{program_id=}")

        program_name = response.xpath("//div[@id='left-menu']//li[contains(@class, 'selected')]//a[contains(@class, 'selected') or contains(@class, 'opened')]/span/text()").getall()
        program_name = program_name[0] if len(program_name) > 0 else program_name
        log.info(f"{program_name=}")

        cycle = "bac" if "focus=100" in response.url or "focus=200" in response.url else "master"
        log.info(f"{cycle=}")

        faculty = response.xpath("//div[@id='left-menu']//li[contains(@class, 'selected')]/a/text()").getall()
        faculty = faculty[0] if len(faculty) > 0 else faculty
        faculties = [faculty]
        log.info(f"{faculties=}")

        # courses_codes = response.xpath("//td[contains(@class, 'code')]/text()").getall()
        courses_links = response.xpath("//td[contains(@class, 'course_title')]/a/@href").getall()
        courses_codes = list(map(lambda x: x.split("KougiCD=")[1].split("&")[0], courses_links))
        log.info(f"{courses_codes=}")

        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": faculties,
            "campuses": [],  # didn't find information on campuses
            "url": response.url,
            "courses": courses_codes,
            "ects": []  # ECTS not applicable in Japan
        }
