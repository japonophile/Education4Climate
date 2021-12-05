from abc import ABC
from pathlib import Path

import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

import json
import logging
log = logging.getLogger()

BASE_URL = 'http://syllabus01.academic.hokudai.ac.jp'
TOP_URL = BASE_URL + '/syllabi/public/syllabus/sylsearch.aspx'
LIST_URL = BASE_URL + '/Syllabi/Public/Syllabus/SylList.aspx'
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}hokkaidou_programs_{YEAR}.json')
CYCLE_MAP = {"02": "bac", "11": "master", "12": "doctor", "13": "grad", "14": "grad"}


class HokkaidoUnivCoursesSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Hokkaido University
    which also outputs programs

    Note: this crawler is different from others because
    the site is in ASPX and does not offer URLs, so we need to issue
    successive POST requests (because the site holds a current state).
    Therefore, we crawl all pages in a chain of requests (quite inefficient
    because the requests are not parallelized and if one request fails, the
    whole parsing fails), and we dump the information about programs at the
    very end (after having crawled all courses).
    """

    name = "hokkaidou-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}hokkaidou_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):
        self.programs = {}
        self.cycles = None
        self.faculties = {}
        self.all_courses_ids = {}
        yield scrapy.Request(TOP_URL, self.select_cycle)

    def validate(self, source):
        # these fields are the minimum required as cannot be hardcoded
        data = {"__VIEWSTATEGENERATOR": source.xpath("//*[@id='__VIEWSTATEGENERATOR']/@value")[0].extract(),
            "__EVENTVALIDATION": source.xpath("//*[@id='__EVENTVALIDATION']/@value")[0].extract(),
            "__VIEWSTATE": source.xpath("//*[@id='__VIEWSTATE']/@value")[0].extract()}
        return data

    def select_cycle(self, response):
        if self.cycles is None:
            self.cycles = [c for c in response.xpath(
                "//select[@id='ctl00_phContents_ucSylSearchuc_ddl_org']/option/@value").getall()
                if "NULL" not in c]
        log.info(f"{self.cycles=}")
        if len(self.cycles) == 0:
            # No cycles left: now dump programs to file
            self.dump_programs()
            log.info("Done!")
            return
        cycle = self.cycles.pop()
        self.programs[cycle] = {}
        log.info(f"cycle: {cycle} ({CYCLE_MAP[cycle]})")
        form_data = self.validate(response)
        form_data["__EVENTTARGET"] = 'ctl00_phContents_ucSylSearchuc_ddl_org'
        form_data["ctl00$phContents$ucSylSearchuc$ddl_org"] = cycle
        yield scrapy.FormRequest.from_response(
            response, formdata=form_data, callback=self.select_faculty,
            method='POST', dont_filter=True, cb_kwargs={'cycle': cycle})

    def select_faculty(self, response, cycle):
        if cycle not in self.faculties:
            faculty_names = [f for f in response.xpath(
                "//select[@id='ctl00_phContents_ucSylSearchuc_ddl_fac']/option/text()").getall()
                if "---" not in f]
            faculty_codes = [f for f in response.xpath(
                "//select[@id='ctl00_phContents_ucSylSearchuc_ddl_fac']/option/@value").getall()
                if "NULL" not in f]
            self.faculties[cycle] = list(zip(faculty_codes, faculty_names))
        log.info(f"{self.faculties=}")
        if len(self.faculties[cycle]) == 0:
            # No faculties left for this cycle, go back to cycle selection
            yield response.follow(TOP_URL, self.select_cycle, dont_filter=True)
            return
        faculty_code, faculty_name = self.faculties[cycle].pop()
        log.info(f"{faculty_code=} {faculty_name=}")
        self.programs[cycle][faculty_code] = {'faculty_name': faculty_name, 'courses': []}
        form_data = self.validate(response)
        form_data["__EVENTTARGET"] = 'ctl00_phContents_ucSylSearchuc_ddl_org'
        form_data["ctl00$phContents$ucSylSearchuc$ddl_fac"] = faculty_code
        yield scrapy.FormRequest.from_response(
            response, formdata=form_data, callback=self.start_search,
            method='POST', dont_filter=True,
            cb_kwargs={'cycle': cycle, 'faculty': faculty_code})

    def start_search(self, response, cycle, faculty):
        log.info("Start search")
        form_data = self.validate(response)
        form_data["__EVENTTARGET"] = 'ctl00_phContents_ucSylSearchuc_ctl109_btnSearch'
        form_data["ctl00$phContents$ucSylSearchuc$ctl109$btnSearch"] = "検索"
        yield scrapy.FormRequest.from_response(
            response, formdata=form_data, callback=self.parse_list,
            method='POST', dont_filter=True,
            cb_kwargs={'cycle': cycle, 'faculty': faculty, 'page': 1, 'course_idx': 0})

    def parse_list(self, response, cycle, faculty, page, course_idx):
        if course_idx == 0:
            # Only show course list on the first time
            log.info(response.xpath(
                "//table[@id='ctl00_phContents_ucSylList_gv']"
                "//tr/td[3]/text()").getall())

        # we can get the list of course titles, but we don't have links
        # to the course description: we need to click the button in ASPX
        # to open the corresponding page....

        # buttons to click on:
        courses = sorted(response.xpath(
            "//table[@id='ctl00_phContents_ucSylList_gv']"
            "//input[@value='Japanese']/@id").getall())

        if course_idx < len(courses):
            form_data = self.validate(response)
            form_data["__EVENTTARGET"] = ""
            form_data[courses[course_idx].replace("_", "$")] = "Japanese"
            yield scrapy.FormRequest.from_response(
                response, formdata=form_data, callback=self.parse_course,
                method='POST', dont_filter=True,
                cb_kwargs={'cycle': cycle, 'faculty': faculty,
                           'page': page, 'course_idx': course_idx})
            return

        next_page = response.xpath(
            f"//td[span/text()='{page}']"
            "//following-sibling::td[1]/a/text()"
        ).get()
        log.info(f"Current page: {page}, next page: {next_page}")

        if next_page is not None:
            # Move to the next page
            form_data = self.validate(response)
            form_data["__EVENTTARGET"] = "ctl00$phContents$ucSylList$gv"
            form_data["__EVENTARGUMENT"] = f"Page${next_page}"
            yield scrapy.FormRequest.from_response(
                response, formdata=form_data, callback=self.parse_list,
                method='POST', dont_filter=True,
                cb_kwargs={'cycle': cycle, 'faculty': faculty,
                        'page': next_page, 'course_idx': 0})
            return

        # No more page left: go back to faculty selection
        yield response.follow(
            TOP_URL, self.select_faculty, dont_filter=True, cb_kwargs={'cycle': cycle})

    def parse_course(self, response, cycle, faculty, page, course_idx):
        try:
            course_id = response.xpath(
                "//span[@id='ctl00_phContents_ucSylDetail_ucSummary_lbl_numbering_cd']/text()").get()
            course_id = course_id.replace(" ", "_").strip()
            if course_id in self.all_courses_ids:
                new_course_id = f"{course_id}_{len(self.all_courses_ids[course_id]) + 1}"
            else:
                self.all_courses_ids[course_id] = []
                new_course_id = f"{course_id}_1"
            self.all_courses_ids[course_id].append(new_course_id)
            log.info(f"course_id={course_id} -> {new_course_id}")
            course_id = new_course_id
            self.programs[cycle][faculty]["courses"].append(course_id)
            course_name = cleanup(" ".join([
                (response.xpath(
                    "//span[@id='ctl00_phContents_ucSylDetail_ucSummary_lbl_sbj_name']/text()").get() or ""),
                (response.xpath(
                    "//span[@id='ctl00_phContents_ucSylDetail_ucSummary_lbl_theme_name']/text()").get() or "")]))
            log.info(f"{course_name=}")
            language = response.xpath(
                "//span[@id='ctl00_phContents_ucSylDetail_ucSummary_lbl_num_language_name']/text()").get()
            languages = []
            if language is not None:
                if "日本語" in language:
                    languages += ['ja']
                if "英語" in language:
                    languages += ['en']
            if len(languages) == 0:
                languages += ['ja']
            log.info(f"{language=} {languages=}")
            teacher = response.xpath(
                "//span[@id='ctl00_phContents_ucSylDetail_ucSummary_lbl_staff_name']/text()").get()
            teachers = []
            if teacher is not None:
                if "<br>" in teacher:
                    teachers = [t.strip() for t in teacher.split("<br>")]
                else:
                    teachers = [teacher]
                teachers = [t.split("(")[0].strip() if "(" in t else t for t in teachers]
                teachers = [t.replace("\u3000", " ") for t in teachers]
            log.info(f"{teacher=} {teachers=}")
            content = cleanup(
                response.xpath(
                    "//span[@id='ctl00_phContents_ucSylDetail_ucContents_ContentSchedule_lblDetail']/text()").get() or "")
            goal = cleanup(
                (response.xpath(
                    "//span[@id='ctl00_phContents_ucSylDetail_ucContents_ContentAim_lblDetail']/text()").get() or "") +
                (response.xpath(
                    "//span[@id='ctl00_phContents_ucSylDetail_ucContents_ContentTarget_lblDetail']/text()").get() or ""))
            activity = response.xpath(
                "//span[@id='ctl00_phContents_ucSylDetail_ucContents_ContentExperience_Note_lblDetail']/text()").get() or ""
            other = response.xpath(
                "//span[@id='ctl00_phContents_ucSylDetail_ucContents_ContentKeyWord_lblDetail']/text()").get() or ""

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

        except Exception as ex:
            log.error(f"An exception occurred while parsing course: {ex}")

        yield response.follow(LIST_URL,
            callback=self.parse_list, dont_filter=True,
            cb_kwargs={'cycle': cycle, 'faculty': faculty,
                       'page': page, 'course_idx': course_idx + 1})

    def dump_programs(self):
        with open(PROG_DATA_PATH, "w") as fp:
            first = True
            fp.write("[\n")
            for cycle in self.programs:
                for faculty_code in self.programs[cycle]:
                    faculty = self.programs[cycle][faculty_code]
                    if len(faculty["courses"]) > 0:
                        program_id = "_".join([cycle, faculty_code])
                        program_name = faculty["faculty_name"]
                        if not first:
                            fp.write(",\n")
                        else:
                            first = False
                        json.dump({
                            "id": program_id,
                            "name": program_name,
                            "cycle": CYCLE_MAP[cycle],
                            "faculties": [program_name],
                            "campuses": [],  # didn't find information on campuses
                            "url": TOP_URL,  # cannot provide direct link to program
                            "courses": list(set(faculty["courses"])),
                            "ects": []  # ECTS not applicable in Japan
                        }, fp, ensure_ascii=False)
            fp.write("\n]\n")
