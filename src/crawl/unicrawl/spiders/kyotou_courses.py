from abc import ABC
from pathlib import Path

import pandas as pd

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://www.z.k.kyoto-u.ac.jp'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}kyotou_programs_{YEAR}.json')


class KyotoUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Kyoto University
    """

    name = "kyotou-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}kyotou_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs = pd.read_json(open(PROG_DATA_PATH, "r"))

        for _, program in programs.iterrows():
            yield scrapy.Request(
                BASE_URL + program["url"].replace('course-list', 'course-detail'),
                cookies={'locale': 'ja'}, callback=self.parse_courses,
                cb_kwargs={"courses": program["courses"]})

    def parse_courses(self, response, courses):

        course_ids = response.xpath("//div[contains(@id, 'lecture_')]/@id").getall()
        for course_id in course_ids:
            if course_id.replace('lecture_', '') not in courses:
                log.warn(f"{course_id} not in program courses")
                continue
            course_name = response.xpath(
                f"//div[@id='{course_id}']//span[@class='course-title']/span[1]/text()").get().strip()

            LANG_MAP = {"日本語": "ja", "英語": "en"}
            language = response.xpath(
                f"//div[@id='{course_id}']//span[@class='language']/text()").get().strip()
            log.info(f"{language=}")
            if '及び' in language:
                languages = [LANG_MAP.get(l, "other") for l in language.split('及び')]
            else:
                languages = [LANG_MAP.get(language, "other")]

            teachers = response.xpath(
                f"//div[@id='{course_id}']//table[@class='teachers']/tr/td[3]/text()").getall()

            def get_sections_text(sections_names):
                texts = [cleanup(response.xpath(
                    f"//div[@id='{course_id}']//div[@class='syllabus-header' and contains(text(), '{section}')]/following-sibling::div[1]").get())
                    for section in sections_names]
                return "\n".join(texts).strip("\n /")

            content = get_sections_text(['(授業計画と内容)'])

            goal = get_sections_text(['(授業の概要・目的)', '(到達目標)'])

            activity = ""

            other = get_sections_text(['(履修要件)', '(成績評価の方法・観点及び達成度)',
                                       '(教科書)', '(参考書等)', '(授業外学習（予習・復習）等)',
                                       '(その他（オフィスアワー等）)'])

            yield {
                "id": course_id.replace('lecture_', ''),
                "name": course_name,
                "year": f"{YEAR}-{int(YEAR)+1}",
                "languages": languages,
                "teachers": teachers,
                "url": response.url + f"#{course_id}",
                "content": content,
                "goal": goal,
                "activity": activity,
                "other": other
            }

        next_page = response.xpath("//nav[@class='pagination']/span[@class='page current']"
                                   "/following-sibling::span[1]/a/@href").get()
        log.info(f"{next_page=}")
        if next_page is not None and len(next_page.strip()) > 0:
            yield response.follow(next_page, self.parse_courses,
                                  cb_kwargs={"courses": courses})
