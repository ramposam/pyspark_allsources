from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, ArrayType
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j

fileschema = StructType([
    StructField("city", StringType()),
    StructField("pop", LongType()),
    StructField("state", StringType()),
    StructField("_id", StringType()),
    StructField("loc", StringType()),
    StructField("CORRUPTED",StringType())
])

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()
    spark = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("Handle bad records using columnNameOfCorruptRecord option.."). \
        getOrCreate()

    logger = Log4j(spark)

    logger.info("Logging bad records while loading file...")
    df = spark.read.json(r'json\zips.json', schema=fileschema,  columnNameOfCorruptRecord='CORRUPTED')
    df.show(10,False)

    spark.stop()
