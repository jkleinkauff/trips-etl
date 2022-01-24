from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import os
import sys
import src.config
import time

if __name__ == "__main__":

    time_str = time.strftime("%Y-%m-%d")
    processed_parquet_dir = os.path.join(
        src.config.PROCESSED_PATH, f"day={time_str}", "trips.parquet"
    )

    # awsAccessKey = sys.argv[1]
    # awsSecretKey = sys.argv[2]

    conf = (
        SparkConf()
        .setAppName("PySpark S3 Integration Example")
        # .set("spark.hadoop.fs.s3a.access.key", awsAccessKey)
        # .set("spark.hadoop.fs.s3a.secret.key", awsSecretKey)
        .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .set("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        .set("spark.speculation", "false")
        .set(
            "spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored",
            "true",
        )
        .set("fs.s3a.experimental.input.fadvise", "random")
        .setIfMissing("spark.master", "local")
    )

    # Creating the SparkSession object
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    # writes to s3 are extremly slow - need to investigate further
    # maybe writing to disk then cp'ying
    spark.sparkContext._jsc.hadoopConfiguration().set(
        "mapreduce.fileoutputcommitter.algorithm.version", "2"
    )

    # session.read.parquet(f's3a:///{bucket}').where(column('day').between('2017-01-02', '2017-01-03'))
    df = spark.read.parquet(processed_parquet_dir)

    df_region = df.select("region").distinct()
    df_datasource = df.select("datasource").distinct()
    df_date = df.select("date", "year", "month", "day").distinct()
    df_trips = df.select(
        "origin_coord_x",
        "origin_coord_y",
        "destination_coord_x",
        "destination_coord_y",
        "date",
        "region",
        "datasource",
        "md5"
    ).distinct()

    # now we extract the data to move to postgres db

    # beaware, coalesce should be avoided when productizing

    df_region.coalesce(1).write.mode("overwrite").csv(
        f"{src.config.DM_DATA_DIR}/d_region.csv"
    )
    df_datasource.coalesce(1).write.mode("overwrite").csv(
        f"{src.config.DM_DATA_DIR}/d_datasource.csv"
    )
    df_date.coalesce(1).write.mode("overwrite").csv(
        f"{src.config.DM_DATA_DIR}/d_date.csv"
    )
    df_trips.coalesce(1).write.mode("overwrite").csv(
        f"{src.config.DM_DATA_DIR}/f_trips_staging.csv"
    )
