from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = 'http://www.ocw.titech.ac.jp/index.php?module=General&action=T0300&JWC={}'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}titech_programs_{YEAR}.json')


class TITechCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for TITech
    """

    name = "titech-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}titech_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        courses_ids = pd.read_json(open(PROG_DATA_PATH, "r"))["courses"]
        courses_ids_list = sorted(list(set(courses_ids.sum())))

        for course_id in courses_ids_list:
            yield scrapy.Request(BASE_URL.format(course_id), self.parse_course,
                                 cb_kwargs={"course_id": str(course_id)})

    def parse_course(self, response, course_id):

        course_name = response.xpath("//h3/text()").get()

        LANG_MAP = {"日本語": "ja", "英語": "en"}
        language = response.xpath("//dt[text()='使用言語']"
                                  "/following::dd[1]/text()").get()
        languages = [LANG_MAP.get(language, "other")]

        teachers = response.xpath("//dt[text()='担当教員名']"
                                  "/following::dd[1]/a/text()").get()

        content = cleanup(response.xpath(f"//div[@id='overview']//h3[contains(text(), '授業計画')]/following::table[1]").get())

        def get_sections_text(sections_names):
            texts = [cleanup(response.xpath(f"//div[@id='overview']//h3[contains(text(), '{section}')]/following::p[1]").get())
                     for section in sections_names]
            return "\n".join(texts).strip("\n /")

        goal = get_sections_text(["ねらい", "目標"])

        activity = ""

        other = get_sections_text(["キーワード", "授業の進め方",
                                   "授業時間外学修", "教科書", "参考書",
                                   "成績評価", "関連する科目", "履修の条件"])

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

