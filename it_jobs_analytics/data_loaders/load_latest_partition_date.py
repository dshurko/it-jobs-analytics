import os
from datetime import date, datetime

from google.cloud import storage

if "data_loader" not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if "test" not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    table_name = os.environ["TABLE_NAME"]
    source = kwargs["source"]

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=f"{table_name}/source={source}")

    latest_date = date(1970, 1, 1)  # start of Unix time
    for blob in blobs:
        if blob.name.endswith(".parquet"):
            date_str = blob.name.split("/")[-2].replace("published_at=", "")
            blob_date = datetime.strptime(date_str, "%Y-%m-%d").date()

            latest_date = max(blob_date, latest_date)

    return latest_date


@test
def test_output(output, *args) -> None:
    assert isinstance(
        datetime.strptime(output, "%Y-%m-%d"), date
    ), "The output must be a date"
