from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, ArrayType,IntegerType,DoubleType
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j

fileschema = StructType([
    StructField("ID", IntegerType()),
    StructField("Name", StringType()),
    StructField("Email", StringType()),
    StructField("Age", IntegerType()),
    StructField("Gender", StringType()),
    StructField("Salary", DoubleType()),
    StructField("corrupted_record",StringType())
])

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()
    spark = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("Handle bad records using bad records path option.."). \
        getOrCreate()

    logger = Log4j(spark)

    logger.info("Logging bad records while loading file...")
    df = spark.read \
    .option("badRecordsPath", "output/badRecordsPath") \
    .option("header","true") \
    .schema(fileschema) \
    .option("columnNameOfCorruptRecord","corrupted_record") \
    .csv(r"csv\baddatafiles\2000000_records.csv")
    df.show()

    spark.stop()
