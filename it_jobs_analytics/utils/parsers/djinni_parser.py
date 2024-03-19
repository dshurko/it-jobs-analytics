from base_parser import BaseParser
from datetime import datetime
from bs4 import BeautifulSoup
import requests


class DjinniParser(BaseParser):
    BASE_URL = "https://djinni.co/jobs/"
    SET_LANG_URL = "https://djinni.co/set_lang?code=en&next=/"
    CATEGORY_MAP = {
        "ai/ml": "ML+AI",
        "data engineering": "Data+Engineer",
        "data science": "Data+Science",
        "java": "Java",
        "node.js": "Node.js",
        "python": "Python",
        "scala": "Scala",
    }

    def __get_job_list_from_page(self, category, page=1):
        category_url = f"{self.BASE_URL}?primary_keyword={self.CATEGORY_MAP[category]}&region=UKR&page={page}"

        response = requests.get(category_url)
        if response.url == self.BASE_URL:
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        li_tags = soup.find_all("li", class_="list-jobs__item job-list__item")

        job_list = []
        for li in li_tags:
            header = li.find("header")
            company = header.find("a", class_="mr-2").text.strip()
            dt = li.find("span", class_="mr-2 nobr")["title"]
            published_at = datetime.strptime(dt, "%H:%M %d.%m.%Y").date()
            title_a_tag = li.find("a", class_="h3 job-list-item__link")
            title = title_a_tag.text.strip()
            url = title_a_tag["href"]

            job_list.append(
                {
                    "category": category,
                    "title": title,
                    "company": company,
                    "published_at": published_at,
                    "url": url,
                }
            )

        return job_list

    def __get_earliest_date(self, job_list):
        return min(job["published_at"] for job in job_list)

    def get_job_list(self, category, start_date, end_date):
        job_list = []

        page = 1
        while True:
            job_list_from_page = self.__get_job_list_from_page(category, page)
            if (
                not job_list_from_page
                or self.__get_earliest_date(job_list_from_page) < start_date
            ):
                for job in job_list_from_page:
                    if (
                        job["published_at"] >= start_date
                        and job["published_at"] <= end_date
                    ):
                        job_list.append(job)
                break
            job_list.extend(job_list_from_page)
            page += 1

        return job_list

    def get_job_description(self, url):
        with requests.Session() as session:
            # set language to English
            session.get(self.SET_LANG_URL)

            response = session.get(url)
            soup = BeautifulSoup(response.text, "html.parser")

            description_div_tags = soup.find_all("div", class_="mb-4")[:2]
            for div in description_div_tags:
                for br in div.find_all("br"):
                    br.replace_with("\n")

            description = "\n\n".join(
                [div.text.strip() for div in description_div_tags]
            )
            return description
