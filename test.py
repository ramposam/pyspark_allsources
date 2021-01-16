from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StringType,StructField,IntegerType
schema = StructType([
     StructField("No",IntegerType()),
     StructField("ID",StringType()),
     StructField("date_field",StringType())
])
if __name__ == "__main__":
    spark = SparkSession.builder.appName("test").getOrCreate()

    df = spark.createDataFrame([(5679862, "HN487108", "07/24/2007 10:11:00 PM")],schema)
    df.show()
    spark.stop()
