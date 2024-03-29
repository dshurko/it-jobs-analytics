from datetime import date, datetime
from typing import Dict, List

import requests
from .base_parser import BaseParser
from bs4 import BeautifulSoup


class DjinniParser(BaseParser):
    """
    A parser for scraping job listings from Djinni.

    Attributes:
        CATEGORY_URL_MAP (dict): A mapping of job categories to their corresponding URLs.
        JOBS_URL (str): The base URL for job listings.
        SET_LANG_URL (str): The URL for setting the language to English.
    """

    CATEGORY_URL_MAP = {
        "Android": "Android",
        "C / C++ / Embedded": "C%2B%2B",
        "C# / .NET": ".NET",
        "Data Engineer": "Data+Engineer",
        "Data Science": "Data+Science",
        "DevOps": "DevOps",
        "Flutter": "Flutter",
        "Fullstack": "Fullstack",
        "Golang": "Golang",
        "Java": "Java",
        "JavaScript / Front-End": "JavaScript",
        "Node.js": "Node.js",
        "PHP": "PHP",
        "Python": "Python",
        "React Native": "React+Native",
        "Ruby": "Ruby",
        "Rust": "Rust",
        "Scala": "Scala",
        "Software Architect": "Architect",
        "iOS": "iOS",
    }
    JOBS_URL = "https://djinni.co/jobs/"
    SET_LANG_URL = "https://djinni.co/set_lang?code=en&next=/"

    def __get_jobs_from_page(self, category: str, page: int = 1) -> List[Dict]:
        """
        Get job listings from a specific category and page.

        Args:
            category (str): The category of jobs to retrieve.
            page (int, optional): The page number of job listings. Defaults to 1.

        Returns:
            List[Dict]: A list of job listings as dictionaries.
        """
        category_url = f"{self.JOBS_URL}?primary_keyword={self.CATEGORY_URL_MAP[category]}&page={page}"
        retries = self.MAX_RETRIES

        while retries > 0:
            try:
                response = requests.get(category_url)
                response.raise_for_status()

                if response.url == self.JOBS_URL:
                    return []

                soup = BeautifulSoup(response.text, "html.parser")
                li_tags = soup.find_all("li", class_="list-jobs__item job-list__item")

                jobs = []

                for li in li_tags:
                    header = li.find("header")
                    company = header.find("a", class_="mr-2").text.strip('\n "«»')
                    dt_str = li.find("span", class_="mr-2 nobr")["title"]
                    published_at = datetime.strptime(dt_str, "%H:%M %d.%m.%Y").date()
                    a_tag = li.find("a", class_="h3 job-list-item__link")
                    title = a_tag.text.strip('\n "«»')
                    url = self.JOBS_URL + a_tag["href"].lstrip("/jobs")

                    jobs.append(
                        {
                            "category": category,
                            "company": company,
                            "published_at": published_at,
                            "source": "djinni",
                            "title": title,
                            "url": url,
                        }
                    )

                return jobs

            except requests.RequestException as e:
                print(f"Request failed: {e}")
                retries -= 1
                if retries > 0:
                    print(f"Retrying... ({self.MAX_RETRIES - retries} attempts left)")
                else:
                    print(
                        f"Failed to fetch jobs from page after {self.MAX_RETRIES} attempts"
                    )
                    return []

    def __get_earliest_date(self, jobs: List[Dict]) -> date:
        """
        Get the earliest published date from a list of job listings.

        Args:
            jobs (List[Dict]): A list of job listings.

        Returns:
            date: The earliest published date.
        """
        return min(job["published_at"] for job in jobs)

    def get_jobs_by_category(
        self, category: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Get job listings by category and within a specified date range.

        Args:
            category (str): The category of jobs to retrieve.
            start_date (date): The start date of the date range.
            end_date (date): The end date of the date range.

        Returns:
            List[Dict]: A list of job listings as dictionaries.
        """
        jobs = []
        page = 1

        while True:
            jobs_from_page = self.__get_jobs_from_page(category, page)
            if not jobs_from_page:
                break

            for job in jobs_from_page:
                if start_date <= job["published_at"] <= end_date:
                    jobs.append(job)

            if self.__get_earliest_date(jobs_from_page) < start_date:
                break

            page += 1

        return jobs

    def get_job_description(self, url: str) -> str:
        """
        Get the description of a job listing.

        Args:
            url (str): The URL of the job listing.

        Returns:
            str: The description of the job listing.
        """
        with requests.Session() as session:
            # set language to English
            session.get(self.SET_LANG_URL)

            response = session.get(url)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            div_tags = soup.find_all("div", class_="mb-4")[:-1]

            description = self.convert_html_to_text(str(div_tags))

            return description
