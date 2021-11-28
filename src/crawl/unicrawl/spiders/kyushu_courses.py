from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URL = 'https://ku-portal.kyushu-u.ac.jp/campusweb/slbssbdr.do?value(risyunen)={}&value(semekikn)=1&value(kougicd)={}&value(crclumcd)=ZZ'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}kyushu_programs_{YEAR}.json')


class KyushuUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Kyushu University
    """

    name = "kyushu-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}kyushu_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        courses_ids = pd.read_json(open(PROG_DATA_PATH, "r"))["courses"]
        courses_ids_list = sorted(list(set(courses_ids.sum())))
        for course_id in courses_ids_list:
            yield scrapy.Request(BASE_URL.format(YEAR, course_id), self.parse_course,
                                 cb_kwargs={"course_id": str(course_id)})
        # for course_id in ["21693162", "S21642103", "21261707", "S21438092",
        #                   "21610044", "21261750", "21425751"]:
        #     yield scrapy.Request(BASE_URL.format(YEAR, course_id), self.parse_course,
        #                         cb_kwargs={"course_id": str(course_id)})


    def parse_course(self, response, course_id):
        def get_sections_text(sections_names):
            texts = ["\n".join(cleanup(response.xpath(
                "//table[@class='syllabus_detail']"
                f"//td[contains(text(), '{section}')]"
                "/following-sibling::td[2]//text()").getall()))
                for section in sections_names]
            return "\n".join(texts) \
                .replace('.none_display {\r\n   display:none;\r\n}', '') \
                .strip("\n /")

        course_name = get_sections_text(["科目名称", "講義科目名"])

        LANG_MAP = {"日本語": "ja", "英語": "en"}
        language = get_sections_text(["使用言語"])
        if language:
            language = language.replace('.', '').strip()
            languages = [LANG_MAP.get(language, "other")]
        else:
            languages = ["ja"]

        teachers = response.xpath(
            "//table[@class='syllabus_detail']"
            f"//td[@class='label_kougi' and contains(text(), '担当教員')]"
            "/following-sibling::td[2]//p/text()").getall()
        teachers = [t.replace('\u3000', ' ').strip() for t in teachers]

        goal = get_sections_text(["授業科目の目的", "授業概要", "個別の教育目標"])

        content = get_sections_text(["授業計画"])
        content += cleanup('\n'.join(response.xpath(
            "//table[@class='syllabus_detail']"
            "//td[@colspan='3']//table//td/text()").getall()))

        activity = ""

        other = get_sections_text(["キーワード"])

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
