
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, LongType, ArrayType,IntegerType,DoubleType
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j
from pyspark.sql.functions import expr, when, to_date, to_timestamp

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()
    spark = SparkSession.builder.config(conf=conf).appName("Large file read and write , observations...")
    logger = Log4j(spark)

    logger.info("Reading crime_data.csv from itverisity labs...")
    df = spark.read. \
        format("csv"). \
        option("header","true"). \
        option("inferSchema","true"). \
        option("encode",'utf-8'). \
        load("/public/crime/csv/crime_data.csv")

    logger.info("All Spark configurations..." + spark.sparkContext.getConf().getAll())

    logger.info("Convert datatypes and datetime columns...")
    df2= df.select(expr("ID").cast("int"),
             expr("`Case Number`").alias("Case_Number"),
             to_timestamp(expr("Date"),'M/d/y h:m:s a').alias("Date"),
             expr("Block"),
             expr("IUCR"),
             expr("`Primary Type`").alias("Primary_Type"),
             expr("Description"),
             expr("`Location Description`").alias("Location_Description"),
             when(expr("Arrest") == "true","Y").otherwise("N").alias("Arrest"),
             expr("Domestic").cast("int"),
             expr("Beat").cast("int"),
             expr("District").cast("int"),
             expr("Ward").cast("int"),
             expr("`Community Area`").cast("int").alias("Community_Area"),
             expr("`FBI Code`").cast("int").alias("FBI_Code"),
             expr("`X Coordinate`").cast("int").alias("X_Coordinate"),
             expr("`Y Coordinate`").cast("int").alias("Y_Coordinate"),
             expr("Year").cast("int"),
             to_timestamp(expr("`Updated On`"),'M/d/y h:m:s a').alias("Updated_On"),
             expr("Latitude").cast("double"),
             expr("Longitude").cast("double"),
             expr("Location"))

    logger.info("Write to Hive partitioned table with parquet and snappy compression")

    df2.write.partitionBy("year","Arrest"). \
        format("parquet"). \
        option("compression","snappy"). \
        mode("overwrite"). \
        saveAsTable("rposam2021_sparkdb.crime_data")

    logger.info("Write to Hive completed..")

    spark.stop()
