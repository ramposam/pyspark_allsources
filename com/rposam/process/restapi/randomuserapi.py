import json
import sys
from operator import mod
from urllib.request import urlopen

import requests

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import col, explode
from com.rposam.util.logger import Log4j
import time
from com.rposam.config.PostgresConf import PostgresConf

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

def writeToPostgres(logger,data):
    rdd = spark.sparkContext.parallelize(data)
    df = spark.read.schema(schema=schema).json(rdd)
    logger.info("Reading json data from rest source")

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

    logger.info("Writing json dataframe to postgres")
    newDF.write.format("jdbc"). \
        options(**postgresConf). \
        mode(saveMode="append"). \
        option("dbtable", "spark.randomuserapi"). \
        save()
if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"

    spark = SparkSession.builder.\
        config(conf=conf). \
        appName("Read json data from restapi and write to jdbc(postgres)"). \
        config("spark.sql.warehouse.dir", warehouseLocation). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport().\
        getOrCreate()
    logger = Log4j(spark)

    postgresConf = PostgresConf().getConnectoin()
    recCount = 1
    data = []
    condition = True
    while (condition):
        jsonDict = requests.get("https://randomuser.me/api/0.8").json()
        data.append(jsonDict)
        # response = urlopen("https://randomuser.me/api/0.8")
        # jsonDict2 = json.loads(response.read())
        if  mod(recCount,100)==0:
            writeToPostgres(logger,data)
            data.clear()
        if recCount==500:
            condition=False
        recCount+=1


    logger.info("Writing to postgres completed")