from pyspark.sql import SparkSession

from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.schema.FileSchema import FileSchema
import sys
import os

from com.rposam.util.logger import Log4j

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()

    Driver = SparkSession. \
        builder. \
        config(conf=conf). \
        appName("ETL Pipeline using Airflow CSV To Parquet"). \
        getOrCreate()

    logger = Log4j(Driver)

    logger.info("Fetching schema of source file")
    schema = FileSchema.empSchema()

    source = sys.argv[1]
    target = sys.argv[2]

    logger.info("Source is {0} and target is {1}".format(source,target))

    logger.info("Started reading data from sources")

    empDF = Driver.read. \
        format("csv"). \
        schema(schema=schema). \
        option("header", "false"). \
        load(source)
    empDF.show()

    logger.info("Source schema is {0}".format(empDF.schema))

    logger.info("Registered source DF as temporarary view")
    empDF.createOrReplaceTempView("emp")

    mgrDF = Driver.sql("""
        select emp.*,
            mgr.ename as manager_name
         from emp 
        left join emp mgr on (emp.mgr = mgr.empno)
    """)

    logger.info("Writing out to a  target initialized..")

    mgrDF.write.\
        format("parquet").\
        mode(saveMode="overwrite").\
        save(target)

    logger.info("writing to target: {0} is completed...".format(target))

    Driver.stop()
