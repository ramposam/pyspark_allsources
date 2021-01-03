import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import col, explode
from com.rposam.util.logger import Log4j

from com.rposam.config.SparkConf import SparkConfiguration

schema = StructType([
    StructField("results", ArrayType(
        StructType([
            StructField("user", StructType([
                StructField("gender", StringType()),
                StructField("name", StructType([
                    StructField("title", StringType()),
                    StructField("first", StringType()),
                    StructField("last", StringType())
                ])),
                StructField("location", StructType([
                    StructField("street", StringType()),
                    StructField("city", StringType()),
                    StructField("state", StringType()),
                    StructField("zip", StringType())
                ])),
                StructField("email", StringType()),
                StructField("username", StringType()),
                StructField("password", StringType()),
                StructField("salt", StringType()),
                StructField("md5", StringType()),
                StructField("sha1", StringType()),
                StructField("sha256", StringType()),
                StructField("registered", StringType()),
                StructField("dob", StringType()),
                StructField("phone", StringType()),
                StructField("cell", StringType()),
                StructField("HETU", StringType()),
                StructField("picture", StructType([
                    StructField("large", StringType()),
                    StructField("medium", StringType()),
                    StructField("thumbnail", StringType())
                ]))
            ]))
        ])
    )),
    StructField("nationality", StringType()),
    StructField("seed", StringType()),
    StructField("version", StringType())
])

if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"

    spark = SparkSession.builder.\
        appName("Read json and write to local installed spark on ubuntu"). \
        config("spark.sql.warehouse.dir", warehouseLocation). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport().\
        getOrCreate()
    logger = Log4j(spark)
    logger.info("Spark session created using enableHivesupport")
    df = spark.read.schema(schema=schema).option("multiLine", "true").json(r"json\randomuserapi.json")

    logger.info("Reading json data using multiline true")

    newDF = df.withColumn("result", explode(col("results"))). \
        withColumn("gender", col("result.user.gender")). \
        withColumn("title", col("result.user.name.title")). \
        withColumn("first_name", col("result.user.name.first")). \
        withColumn("last_name", col("result.user.name.last")). \
        withColumn("street", col("result.user.location.street")). \
        withColumn("city", col("result.user.location.city")). \
        withColumn("state", col("result.user.location.state")). \
        withColumn("zip", col("result.user.location.zip")). \
        withColumn("email", col("result.user.email")). \
        withColumn("username", col("result.user.username")). \
        withColumn("password", col("result.user.password")). \
        withColumn("salt", col("result.user.salt")). \
        withColumn("md5", col("result.user.md5")). \
        withColumn("sha1", col("result.user.sha1")). \
        withColumn("sha256", col("result.user.sha256")). \
        withColumn("registered", col("result.user.registered")). \
        withColumn("dob", col("result.user.dob")). \
        withColumn("phone", col("result.user.phone")). \
        withColumn("cell", col("result.user.cell")). \
        withColumn("HETU", col("result.user.HETU")). \
        withColumn("large", col("result.user.picture.large")). \
        withColumn("medium", col("result.user.picture.medium")). \
        withColumn("thumbnail", col("result.user.picture.thumbnail")). \
        drop("results", "result")

    logger.info("Writing json dataframe to hive")
    newDF.write.insertInto("sparkdb.randomuserapi")

    logger.info("Writing to Hive completed")