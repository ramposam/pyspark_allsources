from pyspark.sql import SparkSession
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j
from com.rposam.schema.FileSchema import FileSchema

schema = FileSchema.empSchema()

if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()
    spark = SparkSession.builder.config(conf=conf).appName("Convert CSV file to a Json").getOrCreate()
    logger = Log4j(spark)
    logger.info("Reading CSV file ..")
    df = spark.read.format("csv").schema(schema=schema).option("header", False).load(r"csv/emp")
    logger.info("Printing schema of the dataframe..")
    df.printSchema()
    logger.info(("Pring sample data of datafame"))
    df.show(5, False)
    df.createOrReplaceTempView("emp")
    logger.info("Converting multiple rows/lines to a single column/json object")
    mgr_df = spark.sql("""
                select collect_list(mgr) as mgr_details
                from ( select 0 as no,struct(manager_name,list_of_employees) as mgr
                from (
                 select m.ename manager_name,collect_list(e.ename) as list_of_employees from emp m
                left join emp e on (m.empno = e.mgr) 
                where m.job = 'MANAGER'
                group by m.ename))
                group by no
            """)
    mgr_df.printSchema()
    logger.info("Writing result to a json file")
    mgr_df.coalesce(1).write.mode("overwrite").format("json").save(r"output/mgr_details.json")
    logger.info("writing to json completed..")
    logger.info(("stopping the application"))

    spark.stop()
    logger.info("Application stopped..")
