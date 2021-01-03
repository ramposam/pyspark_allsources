from pyspark.sql import SparkSession
import sys
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j
import re


def renameAllColumns(cols):
    newCols = []
    for col in cols:
        # Repalce all special symbols,spaces with _
        newCols.append(re.sub("[^a-zA-Z0-9]", "_", col.lower()))
    return newCols


if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    spark = SparkSession.builder. \
        config(conf=conf). \
        appName("Write to AWS S3 bucket"). \
        getOrCreate()

    logger = Log4j(spark)

    logger.info("Reading 10000 Records csv file ")
    df = spark.read. \
        format("com.databricks.spark.csv"). \
        option("inferSchema", "true"). \
        option("header", "true"). \
        option("mode", "DROPMALFORMED"). \
        load(r"csv/10000Records.csv")

    newColumns = renameAllColumns(df.columns)
    logger.info(newColumns)

    logger.info("Convert all column names to lower case")
    renamedDF = df.toDF(*newColumns)

    logger.info("Reading aws credentials from command line..")
    awsAccessKey = sys.argv[1]
    awsSecretKey = sys.argv[2]
    sc = spark.sparkContext
    logger.info("Setting aws credentials using hadoop configuration..")
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # hadoop_conf.set("fs.s3a.access.key", awsAccessKey)
    # hadoop_conf.set("fs.s3a.secret.key",awsSecretKey)
    # hadoop_conf.set("fs.s3a.endpoint", "s3-eu-west-1.amazonaws.com")

    hadoop_conf.set('fs.s3a.access.key', awsAccessKey)
    hadoop_conf.set('fs.s3a.secret.key',awsSecretKey)
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    logger.info("Writing dataframe results to aws s3")

    renamedDF.write. \
    mode("overwrite"). \
    parquet("s3a://awss3bucketcreatedfromec2/output/10000Records")

    logger.info("Writing to S3 completed..")
