from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://ku-portal.kyushu-u.ac.jp'
TOP_URL = BASE_URL + '/campusweb/slbsskgr.do?clearAccessData=true&contenam=slbsskgr&kjnmnNo=7'

# Note: need to change the parameter ROBOTS_OBEY in the crawler settings.py to make the crawler work


class KyushuUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Kyushu University
    """

    name = "kyushu-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}kyushu_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(TOP_URL, self.parse_main)

    def parse_main(self, response):
        program_names = response.xpath("//td[text()='開講学部・学府']"
                                       "/following::td[2]/input/@id").getall()
        program_ids = response.xpath("//td[text()='開講学部・学府']"
                                     "/following::td[2]/input/@value").getall()
        log.info(list(zip(program_names, program_ids)))
        self.courses_ids = {}
        self.programs = list(zip(program_names, program_ids))[::-1]

        yield self.fetch_next_program()

    def fetch_next_program(self):
        program_name, program_id = self.programs.pop()
        log.info(f"Start crawling program: {program_name}, {program_id}")
        self.courses_ids[program_id] = []
        return scrapy.Request(
            TOP_URL, self.start_program_query, dont_filter=True,
            cb_kwargs={"program_name": program_name, "program_id": program_id})

    def start_program_query(self, response, program_name, program_id):
        yield scrapy.FormRequest.from_response(
            response, formid="form", formdata={
                "value(methodname)": "sylkougi_search",
                "buttonName": "searchKougi",
                "value(nendo)": str(YEAR),
                "values(multiKaikoSyozoku)": program_id},
            callback=self.select_max_list_count,
            cb_kwargs={"program_name": program_name, "program_id": program_id})

    def navigate_data(self, page):
        return {
            "buttonName": "",
            "maxDispListCount": "200",
            "value(pageCount)": page,
            "value(maxCount)": "200",
            "navigateKougiList": "dummy",
            "dummy": "dummy"
        }

    def select_max_list_count(self, response, program_name, program_id):
        # Navigate to page "" (= page "1") and set maxDispListCount to "200"
        yield scrapy.FormRequest.from_response(
            response, formid="form", formdata=self.navigate_data(""),
            callback=self.parse_program,
            cb_kwargs={"program_name": program_name, "program_id": program_id})

    def parse_program(self, response, program_name, program_id):
        courses_ids = response.xpath("//tr[contains(@class, 'column')]/td[2]/text()").getall()
        self.courses_ids[program_id] += courses_ids
        log.info(f"{courses_ids=}")

        next_page = response.xpath(
            "//tr[@class='link']/td/div/span/b/following-sibling::a[1]/@onclick").get()

        if next_page is None:
            if len(self.courses_ids[program_id]) > 0:
                cycle = "grad"
                yield {
                    "id": program_id,
                    "name": program_name,
                    "cycle": cycle,
                    "faculties": [program_name],  # could be possible to get faculty in each course description
                    "campuses": [],  # didn't find information on campuses
                    "url": TOP_URL,  # cannot provide direct link to program
                    "courses": list(set(self.courses_ids[program_id])),
                    "ects": []  # ECTS not applicable in Japan
                }

            if len(self.programs) > 0:
                yield self.fetch_next_program()
            return

        # Load next page
        next_page = next_page.split("'pageCount','")[1].split("',")[0]
        log.debug(f"{next_page=}")

        yield scrapy.FormRequest.from_response(
            response, formdata=self.navigate_data(next_page),
            callback=self.parse_program,
            cb_kwargs={"program_name": program_name, "program_id": program_id})
