from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from com.rposam.config.PostgresConf import PostgresConf
from com.rposam.util.logger import Log4j

from com.rposam.config.SparkConf import SparkConfiguration
if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()

    spark = SparkSession.builder.config(conf=conf).appName("Reading zips.json and Writing to Postgresql using JDBC").getOrCreate()

    logger = Log4j(spark)
    logger.info("json file reading strted...")
    df = spark.read.format("json").load(r"json\zips.json")

    logger.info("Convert json dataframe columns to readable format...")
    newDF = df.withColumn("id", expr("_id")). \
        withColumn("longitude", expr("loc[0]")). \
        withColumn("latitude", expr("loc[1]")). \
        drop("_id", "loc")

    logger.info("Reading postgres jdbc connection information..")

    jdbcProps = PostgresConf.getConnectoin()
    logger.info("Writing output to jdbc(postgres)...")

    newDF.write.format("jdbc"). \
        options(**jdbcProps). \
        option("dbtable", "spark.zips"). \
        mode(saveMode="append"). \
        save()
    logger.info("Writing  to jdbc(postgres) is completed ...")
