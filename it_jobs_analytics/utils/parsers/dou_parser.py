from datetime import date, datetime
from typing import Dict, List

import requests
from base_parser import BaseParser
from bs4 import BeautifulSoup


class DouParser(BaseParser):
    """
    A parser for scraping job listings from DOU.

    Attributes:
        CATEGORY_URL_MAP (dict): A mapping of job categories to their corresponding URLs.
        JOBS_URL (str): The base URL for job listings.
        SET_LANG_URL (str): The URL for setting the language to English.
        USER_AGENT (str): The user agent to use for the HTTP requests.
    """

    CATEGORY_URL_MAP = {
        ".NET": ".NET",
        "AI/ML": "AI/ML",
        "Android": "Android",
        "Architect": "Architect",
        "Big Data": "Big Data",
        "Blockchain": "Blockchain",
        "C++": "C%2B%2B",
        "Data Engineer": "Data Engineer",
        "Data Science": "Data Science",
        "DevOps": "DevOps",
        "Embedded": "Embedded",
        "Flutter": "Flutter",
        "Front End": "Front End",
        "Golang": "Golang",
        "Java": "Java",
        "Node.js": "Node.js",
        "PHP": "PHP",
        "Python": "Python",
        "React Native": "React Native",
        "Ruby": "Ruby",
        "Rust": "Rust",
        "Scala": "Scala",
        "iOS/macOS": "iOS/macOS",
    }
    JOBS_URL = "https://jobs.dou.ua/vacancies/"
    SET_LANG_URL = "https://dou.ua/?switch_lang=en"
    USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:101.0) Gecko/20100101 Firefox/101.0"

    def __get_jobs_from_page(self, page_html: str, category: str) -> List[Dict]:
        """
        Extracts job listings from a page HTML.

        Args:
            page_html (str): The HTML content of the page.
            category (str): The category of the job listings.

        Returns:
            List[Dict]: A list of job listings as dictionaries.
        """
        soup = BeautifulSoup(page_html, "html.parser")
        li_tags = soup.find_all("li", class_="l-vacancy")

        jobs = []

        for li in li_tags:
            a_tag = li.find("a", class_="vt")
            title = a_tag.text.strip('\n "«»')
            url = a_tag["href"].rstrip("?from=list_hot")
            company = li.find("a", class_="company").text.strip('\n "«»')
            date_str = li.find("div", class_="date").text.strip()
            published_at = datetime.strptime(date_str, "%d %B %Y").date()

            jobs.append(
                {
                    "category": category,
                    "company": company,
                    "published_at": published_at,
                    "source": "dou",
                    "title": title,
                    "url": url,
                }
            )

        return jobs

    def __get_all_jobs_by_category(self, category: str) -> List[Dict]:
        """
        Retrieves all job listings for a specific category.

        Args:
            category (str): The category of the job listings.

        Returns:
            List[Dict]: A list of job listings as dictionaries.
        """
        try:
            with requests.Session() as session:
                category_url = (
                    f"{self.JOBS_URL}?category={self.CATEGORY_URL_MAP[category]}"
                )
                headers = {"User-Agent": self.USER_AGENT, "Referer": category_url}

                # set language to English
                session.get(self.SET_LANG_URL, headers=headers)

                response = session.get(category_url, headers=headers)

                soup = BeautifulSoup(response.text, "html.parser")
                csrf_token = soup.find("input", {"name": "csrfmiddlewaretoken"})[
                    "value"
                ]

                post_url = f"{self.JOBS_URL}xhr-load/?category={self.CATEGORY_URL_MAP[category]}"

                count = 0
                jobs = []

                while True:
                    data = {
                        "csrfmiddlewaretoken": csrf_token,
                        "count": count,
                    }

                    post_response = session.post(post_url, data=data, headers=headers)
                    response_data = post_response.json()

                    page_html = response_data["html"]

                    jobs_from_page = self.__get_jobs_from_page(page_html, category)
                    jobs.extend(jobs_from_page)

                    if response_data["last"]:
                        break

                    count += 40

                return jobs

        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
            return []

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
        jobs = [
            job
            for job in self.__get_all_jobs_by_category(category)
            if start_date <= job["published_at"] <= end_date
        ]

        return jobs

    def get_job_description(self, url: str) -> str:
        """
        Get the description of a job listing.

        Args:
            url (str): The URL of the job listing.

        Returns:
            str: The description of the job listing.
        """
        headers = {"User-Agent": self.USER_AGENT, "Referer": url}

        try:
            with requests.Session() as session:
                # set language to English
                session.get(self.SET_LANG_URL, headers=headers)

                response = session.get(url, headers=headers)
                response.raise_for_status()

                soup = BeautifulSoup(response.text, "html.parser")
                div_tag = soup.find("div", class_="l-vacancy")

                div_tag.find("h1", class_="g-h2").extract()
                div_tag.find("div", class_="sh-info").extract()
                div_tag.find("div", class_="likely").extract()
                div_tag.find("div", class_="reply").extract()

                description = self.convert_html_to_text(str(div_tag))

                return description

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return ""
