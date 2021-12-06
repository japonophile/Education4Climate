from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://kdb.tsukuba.ac.jp/syllabi/{}/{}/jpn/'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}tsukuba_programs_{YEAR}.json')


class TsukubaUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Tsukuba University
    """

    name = "tsukuba-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tsukuba_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        courses_ids = pd.read_json(open(PROG_DATA_PATH, "r"))["courses"]
        courses_ids_list = sorted(list(set(courses_ids.sum())))

        for course_id in courses_ids_list:
            yield scrapy.Request(BASE_URL.format(YEAR, course_id), self.parse_course,
                                 cb_kwargs={"course_id": str(course_id)})

    def parse_course(self, response, course_id):

        course_name = response.xpath(
            "//h1[@id='course-title']/span[@id='title']/text()").get().strip()

        languages = []
        notes = response.xpath(
            "//div[@id='note-heading-note']/p[@id='note']/text()").get().strip()
        if "英語" in notes:
            languages.append("en")
        else:
            languages.append("ja")

        teachers = response.xpath("//span[@id='assignments']/text()").get().strip()
        if "," in teachers:
            teachers = [t.strip() for t in teachers.split(",")]
        else:
            teachers = [teachers]

        content = cleanup("\n".join([
            response.xpath("//p[@id='summary-contents']/text()").get() or "",
            response.xpath("//div[@id='topics']/text()").get() or ""]))

        goal = cleanup(response.xpath(
            "//h2[contains(text(),'到達目標')]/following-sibling::p[1]/text()").get() or "")

        activity = ""

        other = cleanup(response.xpath(
            "//h2[contains(text(),'キーワード')]/following-sibling::p[1]/text()").get() or "")

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
