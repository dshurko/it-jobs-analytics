from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
import os
from datetime import datetime, date

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    config_path = os.path.join(get_repo_path(), "io_config.yaml")
    config_profile = "default"

    bucket_name = os.environ["GCS_BUCKET_NAME"]
    job_website_name = kwargs["job_website_name"]

    client = GoogleCloudStorage.with_config(
        ConfigFileLoader(config_path, config_profile)
    ).client
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=job_website_name)

    latest_date = date(1970, 1, 1)  # start of Unix time
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            _, year, month, day = blob.name.rstrip(".parquet").split("/")
            date_ = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d").date()
            latest_date = max(date_, latest_date)

    return latest_date


@test
def test_output(output, *args) -> None:
    assert isinstance(
        datetime.strptime(output, "%Y-%m-%d"), date
    ), "The output must be a date"
