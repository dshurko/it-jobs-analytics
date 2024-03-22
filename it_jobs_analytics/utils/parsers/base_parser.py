from abc import ABC, abstractmethod
from concurrent import futures
from datetime import date
from typing import Dict, List


class BaseParser(ABC):
    """
    Base class for job parsers.

    This class defines the common interface and functionality for job parsers.
    Subclasses should implement the abstract methods to provide specific parsing logic.

    Attributes:
        CATEGORY_URL_MAP (dict): A mapping of job categories to their corresponding URLs.
        MAX_RETRIES (int): The maximum number of retries for fetching job descriptions.
    """

    CATEGORY_URL_MAP = {}
    MAX_RETRIES = 3

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
