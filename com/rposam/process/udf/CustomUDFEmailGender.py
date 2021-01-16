from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StringType, IntegerType, DoubleType, DateType, StructType, StructField, BooleanType
import re
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j
from com.rposam.schema.FileSchema import FileSchema
regex = r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)"


def isValidEmail(email):
    if (re.search(regex, str(email))):
        return True
    else:
        return False


def parseGender(gender):
    if str(gender) in ["Male", "M", "male", "m", "MALE"]:
        return "M"
    elif str(gender) in ["Female", "F", "f", "female", "feMale", "FEMALE"]:
        return "F"
    else:
        return "N"


FileSchema = FileSchema()._20000recordsSchema()

if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"
    spark = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("Custom UDF for Email and Gender validations and also a LTI Assignment"). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport(). \
        config("spark.sql.shuffle.partitions", 10). \
        getOrCreate()

    logger = Log4j(spark)
    logger.info("Reading csv file with dropmalformed mode")
    df = spark.read.format("com.databricks.spark.csv"). \
        option("header", "true"). \
        schema(schema=FileSchema). \
        option("mode", "DROPMALFORMED"). \
        load(r"csv\2000000_records.csv")

    df.printSchema()

    logger.info("Creating custome UDF for email and gender")
    isValidEmail = f.udf(isValidEmail, returnType=BooleanType())
    parse_Gender = f.udf(parseGender, returnType=StringType())

    # df.printSchema()
    # df.show()
    logger.info("Repartitioning the file with 10 partitions max ")
    partitionedDF = df.repartition(10)

    logger.info("filtering the file using custome UDF")
    filteredDF = partitionedDF.filter(isValidEmail(f.col("Email")))
    # filteredDF.show()
    parsedDF = filteredDF.withColumn("ParsedGender", parse_Gender(f.col("Gender"))).drop("Gender")
    GenderFilteredDF = parsedDF.filter(f.col("ParsedGender") != "N")

    logger.info("Wrting results to external hdfs location")
    GenderFilteredDF.coalesce(1). \
        write. \
        format("com.databricks.spark.csv"). \
        mode("overwrite"). \
        save(
        "hdfs://localhost:8020/output/2000000_records_filtered")
    logger.info("Writing completed successfully..")
