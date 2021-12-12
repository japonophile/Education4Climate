from abc import ABC
from pathlib import Path

import json
import scrapy
import sys

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

BASE_URL = 'https://syllabus.gs.chiba-u.jp/'
COURSES_PER_PAGE = 100
QUERY_TEMPLATE = (
    "api/result?NENDO={}&GAKKIKBNCD=&KAIKOKBNCD="
    "&KYOSHOKUINNM=&KOGICD=&KAIKO_KAMOKUWEBNM=&NENJICD=&YOBICD="
    "&JIGENCD=&FREEWORD=&NBR_GAKUBUCD=&NBR_GAKKACD=&NBR_SUIJUNCD="
    f"&FUKUSENKO_CD=&COUNT={COURSES_PER_PAGE}"
    "&SORT=&SHOZOKUCD={}&PAGE={}&MODE=false")


class ChibaUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Chiba University
    """

    name = "chibau-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}chibau_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        self.courses_ids = {}
        yield scrapy.Request(BASE_URL + 'api/selectmaster', callback=self.parse_main)

    def parse_main(self, response):
        j = json.loads(response.body.decode('utf8'))
        faculties = j["Data"]["result"]["M_KAIKOKAMOKU_SHOZOKU"]
        faculties = [(f["SHOZOKUCD"], f["SHOZOKUNM"]["String"])
                     for f in faculties if f["SHOZOKUNM"]["Valid"]]
        log.info(faculties)

        for faculty_id, faculty_name in faculties:
            self.courses_ids[faculty_id] = {}
            yield response.follow(
                BASE_URL + QUERY_TEMPLATE.format(YEAR, faculty_id, 1),
                callback=self.parse_program,
                cb_kwargs={"faculty_id": faculty_id, "faculty_name": faculty_name,
                           "page": 1})
        return

    def parse_program(self, response, faculty_id, faculty_name, page):
        j = json.loads(response.body.decode('utf8'))
        if not j["Data"]["syllabusList"]:
            return
        program_ids = list(set([c["KOGI_SHOZOKUCD"] for c in j["Data"]["syllabusList"]]))
        for program_id in program_ids:
            program_courses_ids = [c["KOGICD"] for c in j["Data"]["syllabusList"]
                                   if c["KOGI_SHOZOKUCD"] == program_id]
            self.courses_ids[faculty_id].setdefault(program_id, []).extend(program_courses_ids)

        if page < j["Data"]["total"] / COURSES_PER_PAGE:
            page += 1
            yield response.follow(
                BASE_URL + QUERY_TEMPLATE.format(YEAR, faculty_id, page),
                callback=self.parse_program,
                cb_kwargs={"faculty_id": faculty_id, "faculty_name": faculty_name,
                           "page": page})
            return

        cycle = "grad" if int(faculty_id[-1]) > 1 else "bac"

        for program_id in self.courses_ids[faculty_id]:
            yield {
                "id": program_id,
                "name": faculty_name,
                "cycle": cycle,
                "faculties": [faculty_name],
                "campuses": [],  # didn't find information on campuses
                "url": BASE_URL,
                "courses": list(set(self.courses_ids[faculty_id][program_id])),
                "ects": []  # ECTS not applicable in Japan
            }
