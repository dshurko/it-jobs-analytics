import re
from abc import ABC, abstractmethod
from concurrent import futures
from datetime import date
from typing import Dict, List

import html2text


class BaseParser(ABC):
    """
    Base class for job parsers.

    This class defines the common interface and utility methods for parsing job listings
    from different sources.

    Attributes:
        CATEGORY_URL_MAP (dict): A mapping of job categories to their corresponding URLs.
        MAX_RETRIES (int): The maximum number of retries when fetching job descriptions.
        SPACE_RE (re.Pattern): Regular expression pattern for matching consecutive spaces or tabs.
        LEADING_SPACE_RE (re.Pattern): Regular expression pattern for matching leading spaces at the beginning of a line.
        LEADING_HASH_GT_RE (re.Pattern): Regular expression pattern for matching leading hash or greater-than symbols at the beginning of a line.
        BACKSLASH_DASH_RE (re.Pattern): Regular expression pattern for matching backslash followed by a dash.
        BACKSLASH_PLUS_RE (re.Pattern): Regular expression pattern for matching backslash followed by a plus.
        DASHES_UNDERSCORES_RE (re.Pattern): Regular expression pattern for matching consecutive dashes or underscores.
        SINGLE_NON_SPACE_RE (re.Pattern): Regular expression pattern for matching lines with a single non-space character.
        NEWLINE_SPACE_RE (re.Pattern): Regular expression pattern for matching newlines followed by spaces.
        ORDERED_LIST_RE (re.Pattern): Regular expression pattern for matching ordered list items.
    """

    CATEGORY_URL_MAP = {}
    MAX_RETRIES = 3
    SPACE_RE = re.compile(r"[ \t]+")
    LEADING_SPACE_RE = re.compile(r"^ ", re.MULTILINE)
    LEADING_HASH_GT_RE = re.compile(r"^[#>]+ ", re.MULTILINE)
    BACKSLASH_DASH_RE = re.compile(r"\\-", re.MULTILINE)
    BACKSLASH_PLUS_RE = re.compile(r"\\\+", re.MULTILINE)
    DASHES_UNDERSCORES_RE = re.compile(r"[-_]{2,}")
    SINGLE_NON_SPACE_RE = re.compile(r"^\S\s*$", re.MULTILINE)
    NEWLINE_SPACE_RE = re.compile("\n\s+")
    ORDERED_LIST_RE = re.compile(r"(\d)\\\.")

    @abstractmethod
    def get_jobs_by_category(
        self, category: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Get a list of jobs for a specific category within a given date range.

        Args:
            category (str): The category of jobs to retrieve.
            start_date (date): The start date of the job posting.
            end_date (date): The end date of the job posting.

        Returns:
            List[Dict]: A list of job dictionaries, where each dictionary represents a job.
        """
        pass

    def convert_html_to_text(self, html: str) -> str:
        """
        Convert HTML content to plain text.

        Args:
            html (str): The HTML content to convert.

        Returns:
            str: The plain text content.
        """
        parser = html2text.HTML2Text()
        parser.body_width = 0
        parser.ignore_emphasis = True
        parser.ignore_images = True
        parser.ignore_links = True

        description = parser.handle(html)
        description = self.SPACE_RE.sub(" ", description)
        description = self.LEADING_SPACE_RE.sub("", description)
        description = self.LEADING_HASH_GT_RE.sub("", description)
        description = self.BACKSLASH_DASH_RE.sub("-", description)
        description = self.BACKSLASH_PLUS_RE.sub("+", description)
        description = self.DASHES_UNDERSCORES_RE.sub("", description)
        description = self.SINGLE_NON_SPACE_RE.sub("", description)
        description = self.NEWLINE_SPACE_RE.sub("\n\n", description)
        description = self.ORDERED_LIST_RE.sub(r"\1.", description)
        description = description.replace("\ufeff", "")
        description = description.strip()

        return description

    @abstractmethod
    def get_job_description(self, url: str) -> str:
        """
        Get the description of a job given its URL.

        Args:
            url (str): The URL of the job.

        Returns:
            str: The description of the job.
        """
        pass

    def get_all_jobs(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Get all jobs within a given date range.

        This method retrieves jobs for all categories defined in the `CATEGORY_URL_MAP`
        attribute. It uses multithreading to fetch job descriptions concurrently.

        Args:
            start_date (date): The start date of the job posting.
            end_date (date): The end date of the job posting.

        Returns:
            List[Dict]: A list of job dictionaries, where each dictionary represents a job.
        """
        jobs = []

        for category in self.CATEGORY_URL_MAP.keys():
            jobs.extend(self.get_jobs_by_category(category, start_date, end_date))

        with futures.ThreadPoolExecutor() as executor:
            future_to_job = {
                executor.submit(self.get_job_description, job["url"]): job
                for job in jobs
            }

            for future in futures.as_completed(future_to_job):
                job = future_to_job[future]
                retries = self.MAX_RETRIES

                while retries > 0:
                    try:
                        job["description"] = future.result()
                        break  # if successful, break the retry loop
                    except Exception as e:
                        print(
                            f"An exception occurred while fetching job description: {e}"
                        )
                        retries -= 1
                        if retries > 0:
                            print(
                                f"Retrying... ({self.MAX_RETRIES - retries} attempts left)"
                            )
                        else:
                            print(
                                f"Failed to fetch job description after {self.MAX_RETRIES} attempts"
                            )
                            jobs.remove(
                                job
                            )  # remove job if description couldn't be fetched

        return jobs
