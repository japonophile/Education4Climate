from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = 'https://qsl.cds.tohoku.ac.jp/qsl/syllabus/display/{}'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}tohoku_programs_{YEAR}.json')


class TohokuUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Tohoku University
    """

    name = "tohoku-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tohoku_courses_{YEAR}.json').as_uri()
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
        language = response.xpath("//b[contains(text(), '使用言語') or contains(text(), '開講言語')]"
                                  "/following::text()").get()
        if language is not None:
            language = language.replace('.', '').strip()
            languages = [LANG_MAP.get(language, "other")]
        else:
            languages = ["ja"]

        teachers = response.xpath("//b[contains(text(), '担当教員')]"
                                  "/following::text()").get()
        if teachers is not None:
            teachers = teachers.replace('.', '').strip()
            teachers = teachers.split("所属")[0] if "所属" in teachers else teachers
            if "、" in teachers:
                teachers = [t.strip() for t in teachers.split("、")]
            elif "," in teachers:
                teachers = [t.strip() for t in teachers.split(",")]
            else:
                teachers = [teachers.strip()]
        else:
            teachers = []

        def get_sections_text(sections_names):
            texts = [cleanup(response.xpath(f"//h4[contains(text(), '{section}')]/following::p[1]").get())
                     for section in sections_names]
            return "\n".join(texts).strip("\n /")

        content = get_sections_text(["授業題目", "授業の目的", "授業内容", "授業計画"])

        goal = get_sections_text(["到達目標"])

        activity = ""

        other = get_sections_text(["成績評価方法", "教科書および参考書",
                                   "授業へのパソコン持ち込み", "関連ＵＲＬ",
                                   "授業時間外学修", "その他"])

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
