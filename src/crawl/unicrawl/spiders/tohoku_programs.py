from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import json
import logging
log = logging.getLogger()

BASE_URL = 'https://qsl.cds.tohoku.ac.jp/qsl/'
CYCLE_MAP = { 'b': ('学士', 'bac'), 'm': ('修士', 'master'), 'd': ('博士', 'doctor') }

# Note: need to change the parameter ROBOTS_OBEY in the crawler settings.py to make the crawler work


class TohokuUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Tohoku University
    """

    name = "tohoku-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tohoku_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        yield scrapy.Request(BASE_URL, self.parse_main)

    def parse_main(self, response):
        faculties = response.xpath("//h4/text()").getall()
        log.info(faculties)

        self.courses_ids = {}
        for faculty in faculties:
            faculty_name = faculty.split('・')[0] if '・' in faculty else faculty
            program_link = response.xpath(
                f"//h4[text()='{faculty}']/following::div[1]//a/@href").get()
            program_id = program_link.split("type=")[1]
            self.courses_ids[program_id] = []
            search_url = f"/qsl/syllabus/search_more?skip=0&type={program_id}&query_string="
            yield response.follow(
                search_url, callback=self.parse_program,
                cb_kwargs={"faculty_name": faculty_name, "program_id": program_id})

    def parse_program(self, response, faculty_name, program_id):
        data = json.loads(response.body)
        syllabus_data = data["syllabus_data"]
        courses_ids = [s["_source"]["page"] for s in syllabus_data]
        self.courses_ids[program_id] += courses_ids

        if len(syllabus_data) == 0:
            courses_ids = list(set(self.courses_ids[program_id]))
            for cycle in list(CYCLE_MAP.keys()):
                cycle_courses_ids = [c for c in courses_ids if c[1] == cycle]
                if len(cycle_courses_ids) == 0:
                    continue
                yield {
                    "id": '_'.join([cycle, program_id]),
                    "name": f"{faculty_name}（{CYCLE_MAP[cycle][0]}）",
                    "cycle": CYCLE_MAP[cycle][1],
                    "faculties": [faculty_name],
                    "campuses": [],  # didn't find information on campuses
                    "url": f"{BASE_URL}syllabus/find?type={program_id}",
                    "courses": cycle_courses_ids,
                    "ects": []  # ECTS not applicable in Japan
                }
            return

        n_limit = data["n_limit"]
        skip = data["skip"]
        search_url = response.url.replace(f"skip={skip}", f"skip={skip + n_limit}")

        yield response.follow(
            search_url, callback=self.parse_program,
            cb_kwargs={"faculty_name": faculty_name, "program_id": program_id})
