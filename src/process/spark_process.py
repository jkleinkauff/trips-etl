import sys
import pyspark
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row
import time
import pyspark.sql.functions as F
import src.config

import os


if __name__ == "__main__":

    time_str = time.strftime("%Y-%m-%d")
    staging_parquet_dir = os.path.join(
        src.config.STAGING_PATH, f"day={time_str}", "trips.parquet"
    )
    processed_parquet_dir = src.config.PROCESSED_PATH

    # awsAccessKey = sys.argv[1]
    # awsSecretKey = sys.argv[2]
    # bucketName = sys.argv[3]

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

    df = spark.read.parquet(staging_parquet_dir)

    # process and enrich

    # dealing with "POINT"
    df_processed = df.withColumn(
        "origin_coord",
        F.regexp_extract("origin_coord", "(-?[\d]*\.[\d]*) (-?[\d]*\.[\d]*)", 0),
    )
    # df_processed = df_processed.withColumn(
    #     "origin_coord",
    #     F.regexp_replace("origin_coord"," ", ","),
    # )
    # df_processed = df_processed.withColumn(
    #     "origin_coord",
    #     F.concat(F.lit("POINT("), "origin_coord", F.lit(")"))
    # )

    df_processed = df_processed.withColumn(
        "destination_coord",
        F.regexp_extract("destination_coord", "(-?[\d]*\.[\d]*) (-?[\d]*\.[\d]*)", 0),
    )
    # df_processed = df_processed.withColumn(
    #     "destination_coord",
    #     F.regexp_replace("destination_coord"," ", ","),
    # )
    # df_processed = df_processed.withColumn(
    #     "destination_coord",
    #     F.concat(F.lit("POINT("), "destination_coord", F.lit(")"))
    # )

    # new date and time columns
    split_col_datetime = pyspark.sql.functions.split(df["datetime"], " ")

    df_processed = df_processed.withColumn("date", split_col_datetime.getItem(0))
    df_processed = df_processed.withColumn("year", F.year(df["datetime"]))
    df_processed = df_processed.withColumn("month", F.month(df["datetime"]))
    df_processed = df_processed.withColumn("day", F.dayofmonth(df["datetime"]))
    df_processed = df_processed.withColumn("hour", F.hour(df["datetime"]))

    # origin/destination - x and y - not sure if it makes sense.
    # "origin, destination, and time of day should be grouped together"
    # idk a better way to group then splitting and making it more flexible
    # we probably have a better vanila spark alternative

    split_col_origin = pyspark.sql.functions.split(df_processed["origin_coord"], " ")
    split_col_destination = pyspark.sql.functions.split(
        df_processed["destination_coord"], " "
    )

    df_processed = df_processed.withColumn(
        "origin_coord_x", split_col_origin.getItem(0)
    )
    df_processed = df_processed.withColumn(
        "origin_coord_y", split_col_origin.getItem(1)
    )

    df_processed = df_processed.withColumn(
        "destination_coord_x", split_col_destination.getItem(0)
    )
    df_processed = df_processed.withColumn(
        "destination_coord_y", split_col_destination.getItem(1)
    )

    cols = []
    for i in df_processed.columns:
        cols.append(i)

    df_processed = df_processed.withColumn("md5", F.md5(F.concat_ws("", *cols)))

    df_processed = df_processed.distinct()

    # write parquet
    # for a processed zone, i believe it's ok to have some partitions as we wont be applying other enhancements

    # date, region, datasource

    time_str = time.strftime("%Y-%m-%d")
    df_processed.write.mode("overwrite").partitionBy(
        "date", "region", "datasource"
    ).format("parquet").save(f"{processed_parquet_dir}/day={time_str}/trips.parquet")

    df_processed.show(truncate=False)
