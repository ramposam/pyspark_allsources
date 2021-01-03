from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, split, expr

from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.config.PostgresConf import PostgresConf
from com.rposam.util.logger import Log4j
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} \+\d{4})]'
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\S+)$'
url_pattern = r'\"(\S+)\"'
browser_ip_pattern = r'"(\S+)/(\d.+")$'

# regexp pattern for first element of space deliimter string  '^(?:([^,]*)\,?){1}'
if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()
    spark = SparkSession.\
        builder.\
        config(conf=conf).\
        appName("Read log file and write to postgres").\
        getOrCreate()

    logger = Log4j(spark)

    logger.info("Spark session created..")

    df = spark.read.format("text").load(r"log/bigdataanalyst.in-Jul-2018")

    logs_df = df.select(
                            regexp_extract('value', host_pattern, 1).alias('host'),
                             regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                             regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                             regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                             regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                             regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                             regexp_extract('value', url_pattern, 1).alias('url'),
                             regexp_extract('value', browser_ip_pattern, 1).alias('browser'),
                             regexp_extract('value', browser_ip_pattern, 2).alias('others'))
    logs_df.show(10, truncate=False)

