from abc import ABC
from pathlib import Path

import json
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://syllabus.gs.chiba-u.jp/api/{}/{}/{}/ja_JP'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}chibau_programs_{YEAR}.json')


class ChibaUnivCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Chiba University
    """

    name = "chibau-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}chibau_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        programs = json.load(open(PROG_DATA_PATH, "r"))

        for program in programs:
            for course_id in program["courses"]:
                yield scrapy.Request(BASE_URL.format(YEAR, program["id"], course_id),
                                     self.parse_course,
                                     cb_kwargs={"course_id": str(course_id)})

    def parse_course(self, response, course_id):

        j = json.loads(response.body.decode('utf8'))
        s = j["Data"]["syllabus"]

        course_name = s["KAIKO_KAMOKUWEBNM"]

        languages = {1: ["ja"], 2: ["en"], 3: ["ja", "en"]}.get(s["LANGCD"], ["ja"])

        teachers = [t["KYOSHOKUINNM"].replace("\u3000", " ")
                    for t in j["Data"]["teacherList"]]

        content = "\n".join([s["DESCRIPTION"], s["CLASS_SCHEDULE"]])

        goal = s["GOALS"]

        activity = ""

        other = s["KEYWORDS"]

        yield {
            "id": course_id,
            "name": course_name,
            "year": f"{YEAR}-{int(YEAR)+1}",
            "languages": languages,
            "teachers": teachers,
            "url": response.url.replace("/api", ""),
            "content": content,
            "goal": goal,
            "activity": activity,
            "other": other
        }
