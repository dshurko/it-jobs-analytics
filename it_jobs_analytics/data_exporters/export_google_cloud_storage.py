import os

import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame

if "data_exporter" not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    table_name = os.environ["TABLE_NAME"]
    root_path = f"{bucket_name}/{table_name}"

    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=["source", "published_at"],
        filesystem=gcs,
    )
