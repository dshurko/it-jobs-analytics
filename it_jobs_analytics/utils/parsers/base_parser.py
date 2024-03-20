from abc import ABC, abstractmethod
from concurrent import futures
from datetime import date
from typing import Dict, List


class BaseParser(ABC):
    """
    Base class for job parsers.

    This class defines the common interface and behavior for job parsers.
    Subclasses should implement the abstract methods to provide specific
    parsing logic for different job sources.

    Attributes:
        CATEGORY_MAP (dict): A mapping of job categories to their respective keywords.
        MAX_RETRIES (int): The maximum number of retries when fetching job descriptions.

    """

    CATEGORY_MAP = {}
    MAX_RETRIES = 3

    @abstractmethod
    def get_job_list(
        self, category: str, start_date: date, end_date: date
    ) -> List[Dict]:
        """
        Get a list of job postings for a specific category and date range.

        Args:
            category (str): The job category.
            start_date (date): The start date of the job postings.
            end_date (date): The end date of the job postings.

        Returns:
            List[Dict]: A list of job postings, where each posting is represented as a dictionary.

        """
        pass

    @abstractmethod
    def get_job_description(self, url: str) -> str:
        """
        Get the description of a job posting.

        Args:
            url (str): The URL of the job posting.

        Returns:
            str: The description of the job posting.

        """
        pass

    def get_jobs(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Get a list of job postings for the specified date range.

        This method retrieves the job list for each category defined in the
        CATEGORY_MAP attribute, and then fetches the job description for each
        job in parallel using a ThreadPoolExecutor.

        Args:
            start_date (date): The start date of the job postings.
            end_date (date): The end date of the job postings.

        Returns:
            List[Dict]: A list of job postings, where each posting is represented as a dictionary.

        """
        jobs = []
        for category in self.CATEGORY_MAP.keys():
            jobs.extend(self.get_job_list(category, start_date, end_date))

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
                        retries -= 1  # decrement the retry counter
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
