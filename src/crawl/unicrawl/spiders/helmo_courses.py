# -*- coding: utf-8 -*-
from abc import ABC
from pathlib import Path

import pandas as pd
import scrapy

from src.crawl.utils import cleanup
from settings import YEAR, CRAWLING_OUTPUT_FOLDER

BASE_URl = "https://www.helmo.be/Formations/{}"
PROG_DATA_PATH = Path(__file__).parent.absolute().joinpath(
    f'../../../../{CRAWLING_OUTPUT_FOLDER}helmo_programs_{YEAR}.json')

LANGUAGES_DICT = {
    "Français": 'fr',
    "Anglais": 'en'
}


class HELMOCourseSpider(scrapy.Spider, ABC):
    """
    Courses crawler for Haute Ecole Libre Mosane
    """

    name = "helmo-courses"
    custom_settings = {
        'FEED_URI': Path(__file__).parent.absolute().joinpath(
            f'../../../../{CRAWLING_OUTPUT_FOLDER}helmo_courses_{YEAR}.json').as_uri()
    }

    def start_requests(self):

        courses_df = pd.read_json(open(PROG_DATA_PATH, "r"))[["courses", "courses_urls"]]
        # Combine lists of strings
        courses_ids_list = courses_df["courses"].sum()
        courses_urls_list = courses_df["courses_urls"].sum()
        # Some courses are specified at two different urls which have exactly the same content
        courses_ds = pd.Series(courses_ids_list, courses_urls_list).drop_duplicates()

        for course_url, course_id in courses_ds.items():
            yield scrapy.Request(BASE_URl.format(course_url), self.parse_main, cb_kwargs={"ue_id": str(course_id)})

    @staticmethod
    def parse_main(response, ue_id):

        ue_name = cleanup(response.xpath("//section[@id='helmoContent']//h3/text()").get())
        main_teacher = response.xpath("//div[text()=\"Responsable de l'UE :\"]/following::span[1]/text()").get()
        sub_teachers = response.xpath("//div[text()=\"Autres intervenants :\"]/following::span[1]/text()").get()
        teachers = [main_teacher]
        if sub_teachers:
            teachers += sub_teachers.split(",")
        teachers = [t.strip(" ").lower().title() for t in teachers]
        # Put surname first
        teachers = [f"{' '.join(t.split(' ')[1:])} {t.split(' ')[0]}" for t in teachers]

        years = response.xpath("//div[text()=\"Année académique :\"]/following::span[1]/text()").get()

        languages = response.xpath("//div[text()=\"Langue d'enseignement :\"]/following::span[1]/text()").get()
        languages = [LANGUAGES_DICT[languages]]

        def get_sections_text(sections_names):
            texts = [cleanup(response.xpath(f"//h4[contains(text(), \"{section_name}\")]/following::div[1]").get())
                     for section_name in sections_names]
            return "\n".join(texts).strip("\n")
        content = get_sections_text(['Contenu'])
        goal = get_sections_text(['Objectifs', 'Acquis'])
        activity = get_sections_text('Dispositif')

        yield {
            'id': ue_id,
            'name': ue_name,
            'year': years,
            'languages': languages,
            'teachers': teachers,
            'url': response.url,
            'content': content,
            'goal': goal,
            'activity': activity,
            'other': ''
        }
