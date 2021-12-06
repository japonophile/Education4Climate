from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://kdb.tsukuba.ac.jp'
CYCLE_MAP = {"288": "bac", "441": "bac", "1025": "grad", "6691": "grad"}


class TsukubaUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Tsukuba University
    """

    name = "tsukuba-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tsukuba_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        self.courses_ids = {}
        self.h1 = None
        self.h2 = {}
        yield scrapy.Request(url=BASE_URL, callback=self.select_hierarchy1)

    def select_hierarchy1(self, response):
        if self.h1 is None:
            h1_codes = response.xpath("//select[@id='hierarchy1']/option/@value").getall()
            h1_names = response.xpath("//select[@id='hierarchy1']/option/text()").getall()
            self.h1 = [h for h in list(zip(h1_codes, h1_names)) if h[0] != ""]
            log.info(f"{self.h1=}")

        if len(self.h1) > 0:
            h1_code, h1_name = self.h1.pop()
            self.courses_ids[h1_code] = {}
            yield scrapy.FormRequest.from_response(
                response, formid="listForm", formdata={
                    "_eventId": "changeHierarchySet",
                    "nendo": str(YEAR),
                    "index": "1",
                    "hierarchy1": h1_code},
                dont_filter=True, callback=self.select_hierarchy2,
                cb_kwargs={"h1_code": h1_code, "h1_name": h1_name})
        else:
            log.info("Done!")

    def select_hierarchy2(self, response, h1_code, h1_name):
        if h1_code not in self.h2:
            h2_codes = response.xpath("//select[@id='hierarchy2']/option/@value").getall()
            h2_names = response.xpath("//select[@id='hierarchy2']/option/text()").getall()
            self.h2[h1_code] = [h for h in list(zip(h2_codes, h2_names)) if h[0] != ""]
            log.info(f"{self.h2[h1_code]=}")

        if len(self.h2[h1_code]) > 0:
            h2_code, h2_name = self.h2[h1_code].pop()
            self.courses_ids[h1_code][h2_code] = []
            yield scrapy.FormRequest.from_response(
                response, formid="listForm", formdata={
                    "_eventId": "changeHierarchySet",
                    "nendo": str(YEAR),
                    "index": "2",
                    "hierarchy1": h1_code,
                    "hierarchy2": h2_code},
                dont_filter=True, callback=self.start_search,
                cb_kwargs={"h1_code": h1_code, "h1_name": h1_name,
                           "h2_code": h2_code, "h2_name": h2_name})
        else:
            # No hierarchy2 left: go back to hierarchy1 selection
            yield response.follow(
                url=BASE_URL, dont_filter=True, callback=self.select_hierarchy1)

    def start_search(self, response, h1_code, h1_name, h2_code, h2_name):
        yield scrapy.FormRequest.from_response(
            response, formid="listForm", formdata={
                "_eventId": "searchOpeningCourse",
                "locale": "",
                "nendo": str(YEAR),
                "index": "",
                "termCode": "",
                "dayCode": "",
                "periodCode": "",
                "campusCode": "",
                "hierarchy1": h1_code,
                "hierarchy2": h2_code,
                "hierarchy3": "",
                "hierarchy4": "",
                "hierarchy5": "",
                "freeWord": "",
                "_orFlg": "1",
                "_andFlg": "1",
                "_gaiyoFlg": "1",
                "_risyuFlg": "1",
                "_excludeFukaikoFlg": "1",
                "outputFormat": "1"
            },
            meta={"download_timeout": 60},
            dont_filter=True, callback=self.parse_list,
            cb_kwargs={"h1_code": h1_code, "h1_name": h1_name,
                       "h2_code": h2_code, "h2_name": h2_name})

    def parse_list(self, response, h1_code, h1_name, h2_code, h2_name):
        courses_ids = response.xpath("//td[@class='courseWidth']/text()").getall()
        courses_ids = [i.strip() for i in courses_ids]
        log.info(f"{courses_ids=}")
        self.courses_ids[h1_code][h2_code] += courses_ids

        next_page = response.xpath("//td/a[contains(text(),'次へ')]/@href").get()
        log.info(f"{next_page=}")
        if next_page:
            # If we have other pages for this program, crawl them first
            yield response.follow(
                next_page, meta={"download_timeout": 60},
                dont_filter=True, callback=self.parse_list,
                cb_kwargs={"h1_code": h1_code, "h1_name": h1_name,
                           "h2_code": h2_code, "h2_name": h2_name})
            return

        # No page left, issue a program entry
        h1h2_courses = list(set(self.courses_ids[h1_code][h2_code]))
        if len(h1h2_courses) > 0:
            program_id = "_".join([h1_code, h2_code])
            program_name = " ".join([h1_name, h2_name])
            cycle = CYCLE_MAP[h1_code]
            faculties = [program_name]  # could not find faculties
            yield {
                "id": program_id,
                "name": program_name,
                "cycle": cycle,
                "faculties": faculties,
                "campuses": [],  # didn't find information on campuses
                "url": BASE_URL,  # don't have direct URL to programs
                "courses": h1h2_courses,
                "ects": []  # ECTS not applicable in Japan
            }

        # Go back to hierarchy2 selection
        yield response.follow(
            url=BASE_URL, dont_filter=True, callback=self.select_hierarchy2,
            cb_kwargs={"h1_code": h1_code, "h1_name": h1_name})
