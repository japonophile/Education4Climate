from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://catalog.he.u-tokyo.ac.jp/detail?code={}&year={}'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}tokyou_programs_{YEAR}.json')


class TokyoUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Tokyo University
    """

    name = "tokyou-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tokyou_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        courses_ids = pd.read_json(open(PROG_DATA_PATH, "r"))["courses"]
        courses_ids_list = sorted(list(set(courses_ids.sum())))

        for course_id in courses_ids_list:
            yield scrapy.Request(BASE_URL.format(course_id, YEAR), self.parse_course,
                                 cb_kwargs={"course_id": str(course_id)})

    def parse_course(self, response, course_id):

        course_name = response.xpath("//h1/text()").get().strip()

        LANG_MAP = {"日本語": "ja", "英語": "en"}
        language = response.xpath("//div[contains(text(), '使用言語')]"
                                  "/following::div[1]/text()").get().strip()
        log.info(f"{language=}")
        if '/' in language:
            languages = [LANG_MAP.get(l, "other") for l in language.split('/')]
        else:
            languages = [LANG_MAP.get(language, "other")]

        teachers = [t.strip() for t in response.xpath(
            "//div[contains(@class, 'lecturer-cell')]/text()").getall()]
        teachers = [t for t in teachers if t != "教員"]

        content = cleanup(response.xpath(f"//div[@class='catalog-page-detail-card-body-pre']/text()").get())

        goal = cleanup(response.xpath("//div[@class='catalog-page-detail-lecture-aim']/text()").get())

        activity = ""

        other = ""

        yield {
            "id": course_id,
            "name": course_name,
            "year": f"{YEAR}-{int(YEAR)+1}",
            "languages": languages,
            "teachers": teachers,
            "url": response.url,
            "content": content,
            "goal": goal,
            "activity": activity,
            "other": other
        }
