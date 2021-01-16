import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import col, explode
from com.rposam.util.logger import Log4j
from com.rposam.schema.FileSchema import FileSchema
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.config.HiveConf import HiveConfiguration
schema = FileSchema.randomuserapiSchema()

if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"

    spark = SparkSession.builder.\
        appName("Read json and write to local installed hive using jdbc"). \
        config("spark.sql.warehouse.dir", warehouseLocation). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport().\
        getOrCreate()
    logger = Log4j(spark)
    logger.info("Spark session created using enableHivesupport")
    df = spark.read.schema(schema=schema).option("multiLine", "true").json(r"json\randomuserapi.json")

    logger.info("Reading json data using multiline true")
    df.printSchema()
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

    newDF.printSchema()
    hiveconf = HiveConfiguration().getConnectoin()
    logger.info("Writing json dataframe to hive using jdbc")
    newDF.show()
    newDF.write.\
        format("jdbc").\
        options(**hiveconf).\
        option("dbtable","sparkdb.randomuserapi").\
        mode(saveMode="overwrite").\
        save()

    logger.info("Writing to Hive completed")