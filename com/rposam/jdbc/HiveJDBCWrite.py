import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
from pyspark.sql.functions import col, explode
from com.rposam.util.logger import Log4j
from com.rposam.schema.FileSchema import FileSchema
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.config.HiveConf import HiveConfiguration

schema = FileSchema.empSchema()

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
    df = spark.read.schema(schema=schema).option("multiLine", "true").json(r"csv\emp")

    logger.info("Reading json data using multiline true")
    df.printSchema()

    hiveconf = HiveConfiguration().getConnectoin()
    logger.info("Writing csv dataframe to hive using jdbc")
    df.write.\
        format("jdbc").\
        options(**hiveconf).\
        option("dbtable","sparkdb.emp").\
        mode(saveMode="append").\
        save()

    logger.info("Writing to Hive completed")