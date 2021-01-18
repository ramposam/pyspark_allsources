import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from com.rposam.config.PostgresConf import PostgresConf
from com.rposam.config.SparkConf import SparkConfiguration
import sys
import os
from com.rposam.schema.FileSchema import FileSchema
from com.rposam.util.logger import Log4j

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()

    Driver = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("ETL Pipeline using Airflow Parquet To AVRO "). \
        getOrCreate()

    # Read avro schema definition from user defined file
    # avroSchema = open("schema/emp_mgr_avro.avsc","r").read()
    logger = Log4j(Driver)
    logger.info("Spark session created...")


    source = sys.argv[1]
    target = sys.argv[2]

    logger.info(f"Source is {source} and target is {target}")

    logger.info("Started reading data from sources")

    # Use userdefined avro schema and read the file
    # empMgrDF = Driver.read. \
    #     format("avro"). \
    #     option("avroSchema", str(avroSchema)). \
    #     load(source)

    empMgrDF = Driver.read. \
        format("avro"). \
        load(source)

    logger.info(f"Fetching schema of source file: {empMgrDF.schema}")
    postgresConf = PostgresConf.getConnectoin()

    logger.info("Writing out to a  target initialized..")
    empMgrDF.printSchema()
    empMgrDF.show()
    empMgrDF.write. \
        format("jdbc"). \
        options(**postgresConf). \
        option("dbTable", target). \
        mode(saveMode="append"). \
        save()

    logger.info(f"writing to target: {target} is completed...")

    Driver.stop()
