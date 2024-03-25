from datetime import date, datetime, timedelta

from pandas import DataFrame

from it_jobs_analytics.utils.parsers.djinni_parser import DjinniParser

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data(latest_partition_date: str, **kwargs) -> DataFrame:
    start_date = datetime.strptime(
        latest_partition_date, "%Y-%m-%d"
    ).date() + timedelta(days=1)
    end_date = kwargs.get("execution_date").date() - timedelta(days=1)

    print(start_date, end_date)

    parser = DjinniParser()
    jobs = parser.get_all_jobs(start_date, end_date)

    return DataFrame(jobs)


@test
def test_output(output, *args) -> None:
    assert output is not None, "The output is undefined"
