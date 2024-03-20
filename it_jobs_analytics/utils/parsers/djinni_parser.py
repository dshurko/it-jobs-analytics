import re
from base_parser import BaseParser
from datetime import datetime, date
from bs4 import BeautifulSoup
import requests
from typing import List, Dict


class DjinniParser(BaseParser):
    """
    A parser for extracting job listings and descriptions from Djinni.
    """

    JOBS_URL = "https://djinni.co/jobs/"
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

    def _get_job_list_from_page(self, category: str, page: int = 1) -> List[Dict]:
        """
        Get a list of job listings from a specific category and page.

        Args:
            category (str): The category of jobs to retrieve.
            page (int, optional): The page number to retrieve. Defaults to 1.

        Returns:
            List[Dict]: A list of dictionaries representing job listings.
        """
        category_url = f"{self.JOBS_URL}?primary_keyword={self.CATEGORY_MAP[category]}&region=UKR&page={page}"

        try:
            response = requests.get(category_url)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return []

        if response.url == self.JOBS_URL:
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
            url = self.JOBS_URL + title_a_tag["href"].lstrip("/jobs")

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

    def _get_earliest_date(self, job_list: List[Dict]) -> date:
        """
        Get the earliest published date from a list of job listings.

        Args:
            job_list (List[Dict]): A list of dictionaries representing job listings.

        Returns:
            date: The earliest published date.
        """
        return min(job["published_at"] for job in job_list)

    def get_job_list(
        self, category: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Get a list of job listings within a specified date range.

        Args:
            category (str): The category of jobs to retrieve.
            start_date (date): The start date of the range.
            end_date (date): The end date of the range.

        Returns:
            List[Dict]: A list of dictionaries representing job listings.
        """
        job_list = []

        page = 1
        while True:
            job_list_from_page = self._get_job_list_from_page(category, page)
            if (
                not job_list_from_page
                or self._get_earliest_date(job_list_from_page) < start_date
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

    def get_job_description(self, url: str) -> str:
        """
        Get the description of a job listing from its URL.

        Args:
            url (str): The URL of the job listing.

        Returns:
            str: The description of the job listing.
        """
        with requests.Session() as session:
            # set language to English
            session.get(self.SET_LANG_URL)

            try:
                response = session.get(url)
                response.raise_for_status()
            except requests.RequestException as e:
                print(f"Request failed: {e}")
                return ""

            soup = BeautifulSoup(response.text, "html.parser")

            description_div_tags = soup.find_all("div", class_="mb-4")[:2]
            for div in description_div_tags:
                for br in div.find_all("br"):
                    br.replace_with("\n")

            description = "\n".join([div.text.strip() for div in description_div_tags])
            description = re.sub(r"[ \t]+", " ", description)
            description = re.sub(r"( *\n *)+", "\n", description)

            return description
