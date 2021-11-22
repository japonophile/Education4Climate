from abc import ABC
from pathlib import Path

import scrapy

from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import logging
log = logging.getLogger()

FACULTIES_URL = 'https://www.u-tokyo.ac.jp/ja/schools-orgs/faculties/faculty.html'
BASE_URL = 'https://catalog.he.u-tokyo.ac.jp/result?q=&type=all&faculty_id='
QUERY_TEMPLATE = '&facet=%7B%22faculty_type%22%3A%5B%22{}%22%5D%2C%22faculty_id%22%3A%5B%22{}%22%5D%7D&page={}'
COMMON_TRUNK = '26'
FACULTY_TYPES = ['ug', 'g']
FACULTY_TYPE_TO_CYCLE = {'jd': 'bac', 'ug': 'bac', 'g': 'grad'}

# Note: need to change the parameter ROBOTS_OBEY in the crawler settings.py to make the crawler work


class TokyoUnivProgramSpider(scrapy.Spider, ABC):
    """
    Programs crawler for Tokyo University
    """

    name = "tokyou-programs"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}tokyou_programs_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        self.courses_ids = dict(list(zip(FACULTY_TYPES, [{}]*len(FACULTY_TYPES))))
        self.faculties_map = {}

        # First check faculties
        yield scrapy.Request(url=FACULTIES_URL, callback=self.parse_faculties)

    def parse_faculties(self, response):
        # Populate a faculties map
        faculties = [s.strip() for s in response.xpath("//h2/text()").getall()]
        for f in faculties:
            if '・' in f:
                k, v = f.split('・')
                self.faculties_map[k] = v

        # Now go to OCW home
        yield scrapy.Request(url=BASE_URL, callback=self.parse_programs)

    def parse_programs(self, response):
        faculty_ids = response.xpath(
            "//div[contains(@class, 'catalog-facet-item') and contains(span/text(), '学部・研究科')]"
            "/following::div[1]//input[@name='faculty_id']/@value").getall()
        faculty_ids = [i for i in faculty_ids if i != COMMON_TRUNK]
        log.info(faculty_ids)

        yield self.parse_common_trunk(response)

        for faculty_type in FACULTY_TYPES:
            for faculty_id in faculty_ids:
                self.courses_ids[faculty_type].setdefault(faculty_id, [])
                yield self.follow_faculty_link(response, faculty_type, faculty_id, 1)

    def parse_common_trunk(self, response):
        faculty_type = 'jd'
        faculty_id = COMMON_TRUNK
        self.courses_ids['jd'] = {COMMON_TRUNK: []}
        return self.follow_faculty_link(response, faculty_type, faculty_id, 1)

    def follow_faculty_link(self, response, faculty_type, faculty_id, page):
        faculty_link = BASE_URL + QUERY_TEMPLATE.format(faculty_type, faculty_id, page)
        return response.follow(
            faculty_link, self.parse_faculty,
            cb_kwargs={'faculty_type': faculty_type, 'faculty_id': faculty_id, 'page': page})

    def parse_faculty(self, response, faculty_type, faculty_id, page):
        course_codes = response.xpath("//div[@class='catalog-search-result-table-cell code-cell']/div[1]/text()").getall()
        self.courses_ids[faculty_type][faculty_id] += course_codes

        next_page = response.xpath("//a[contains(@class, 'catalog-page-link-active')]"
                                   "/following::a[1]/text()").get()
        log.info(f"{next_page=}")
        if next_page is not None and len(next_page.strip()) > 0:
            yield self.follow_faculty_link(response, faculty_type, faculty_id, next_page)
            return

        if len(self.courses_ids[faculty_type][faculty_id]) == 0:
            return

        program_id = '_'.join([faculty_type, faculty_id])
        cycle = FACULTY_TYPE_TO_CYCLE[faculty_type]
        faculty = response.xpath(
            "//div[contains(@class, 'catalog-facet-item') and contains(span/text(), '学部・研究科')]"
            "/following::div[1]//span/text()").get()
        if faculty is None:
            log.warning(f"faculty is None, but have {len(self.courses_ids[faculty_type][faculty_id])} course IDs: {self.courses_ids[faculty_type][faculty_id]}")
            return
        faculty = faculty.strip()
        degree = response.xpath(
            "//div[contains(@class, 'catalog-facet-item') and contains(span/text(), '課程')]"
            "/following::div[1]//span/text()").get()
        if degree is None:
            log.warning(f"degree is None, but have {len(self.courses_ids[faculty_type][faculty_id])} course IDs: {self.courses_ids[faculty_type][faculty_id]}")
            return
        degree = degree.strip()
        program_name = f"{faculty} ({degree})"
        # Map specialty to faculaty (for graduate school)
        faculty = self.faculties_map.get(faculty, faculty)
        faculties = [faculty]
        yield {
            "id": program_id,
            "name": program_name,
            "cycle": cycle,
            "faculties": faculties,
            "campuses": [],  # didn't find information on campuses
            "url": BASE_URL + QUERY_TEMPLATE.format(faculty_type, faculty_id, 1),
            "courses": list(set(self.courses_ids[faculty_type][faculty_id])),
            "ects": []  # ECTS not applicable in Japan
        }
