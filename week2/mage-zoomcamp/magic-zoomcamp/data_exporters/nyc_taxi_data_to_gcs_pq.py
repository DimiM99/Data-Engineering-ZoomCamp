import os

import pyarrow as pa
import pyarrow.parquet as pq
from pandas import DataFrame

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/personal-gcp.json"

project_id = "peerless-sensor-411315"
bucket_name = "mage-zoomcamp-peerless-sensor-411315"

table_name = "nyc_taxi_data"
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:

    pa_table = pa.Table.from_pandas(df)
    gcs_fs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table=pa_table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem=gcs_fs
    )
