# as required, the final solution should not use this file-step into consideration

from distutils.command import config
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import time
import os
import src.config


def read_csv(path):
    data = pd.read_csv(path)
    return data


def csv_to_parquet(df, destination):
    try:
        table = pa.Table.from_pandas(df)
        pq.write_to_dataset(
            table,
            root_path=destination
            # partition_cols=['datetime'],
        )
    except Exception as e:
        logging.error("Error while converting CSV file to parquet:", str(e))


def create_spark_dir(obj_key):
    time_str = time.strftime("%Y-%m-%d")
    ftime_str = time.strftime("%Y%m%d-%H%M%S")
    return f"day={time_str}/{obj_key}.parquet"


if __name__ == "__main__":

    dir = create_spark_dir("trips")
    csv_path = src.config.CSV_PATH
    destination = os.path.join(src.config.STAGING_PATH, create_spark_dir("trips"))

    # import pdb

    # pdb.set_trace()

    if src.config.BACKEND == "local":
        os.makedirs(os.path.dirname(destination), exist_ok=True)

    csv_data = read_csv(csv_path)

    # convert csv to parquet
    csv_to_parquet(csv_data, destination)
    print(csv_data.head())
