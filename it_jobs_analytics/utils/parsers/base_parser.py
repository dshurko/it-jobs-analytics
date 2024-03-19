from abc import ABC, abstractmethod


class BaseParser(ABC):
    CATEGORY_MAP = {}

    @abstractmethod
    def get_job_list(self, category, start_date, end_date):
        pass

    @abstractmethod
    def get_job_description(self, url):
        pass
