import json
import sys
from operator import mod
from urllib.request import urlopen

import requests

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, ArrayType, StringType, IntegerType
from pyspark.sql.functions import col, explode, expr, split, regexp_extract
from com.rposam.util.logger import Log4j
import time
from com.rposam.config.PostgresConf import PostgresConf

from com.rposam.config.SparkConf import SparkConfiguration

if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"

    spark = SparkSession.builder. \
        config(conf=conf). \
        appName("Read .dat  file and write to hive"). \
        config("spark.sql.warehouse.dir", warehouseLocation). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    logger = Log4j(spark)

    df = spark.read.format("text").option("encode", "utf-8").load(r"dat/movies.dat")
    df.printSchema()

    logger.info(("Splitting dataframe columns with double/multiple delimiter.."))


    doubleDelimterSplitDF = df.withColumn("data", split(col("value"), "::")). \
        select(expr("data[0]").cast(IntegerType()).alias("Movie_id"),
               expr("data[1]").alias("Movie_Title"),
               expr("data[2]").alias("geners"))

    logger.info("Further splitting dataframe column with single delimiter as pipe")
    furtherSplitDF = doubleDelimterSplitDF. \
        withColumn("exploded_geners", explode(split(col("geners"), "\|+"))).drop("geners")

    logger.info("Extracting value from bracket using regular expressions")
    regexpExtractDF = furtherSplitDF. \
        withColumn("year", regexp_extract(col("Movie_Title"), r"\(([^()]+)\)$", 1))

    regexpExtractDF.show(truncate=False)

    logger.info("Writing to Hive database SparkDB")
    regexpExtractDF.write.mode("overwrite").saveAsTable("sparkdb.movies")

    logger.info("Writing to Hive completed")
