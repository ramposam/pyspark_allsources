from pyspark.sql import SparkSession

from com.rposam.config.SparkConf import SparkConfiguration
import sys
import os

from com.rposam.util.logger import Log4j

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()

    Driver = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("ETL Pipeline using Airflow Parquet To AVRO "). \
        getOrCreate()

    logger = Log4j(Driver)

    source = sys.argv[1]
    target = sys.argv[2]

    logger.info("Source is {0} and target is {1}".format(source,target))

    logger.info("Started reading data from sources")

    empMgrDF = Driver.read. \
        format("parquet"). \
        load(source)

    logger.info("Fetching schema of source file: {0}".format(empMgrDF.schema))

    logger.info("Writing out to a  target initialized..")
    empMgrDF.printSchema()
    empMgrDF.show()
    empMgrDF.write.\
        format("avro").\
        mode(saveMode="overwrite").\
        save(target)

    logger.info("writing to target: {0} is completed...".format((target)))

    Driver.stop()
