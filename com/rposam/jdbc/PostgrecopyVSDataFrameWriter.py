import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
import psycopg2
import os
import sys
import configparser as cp
from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.schema.FileSchema import FileSchema


class PostgrecopyVSDataFrameWriter(object):

    def time_test(self,func):
        t1 = time.time()
        func()
        t2 = time.time()
        print("==============================================================")
        print(str(t2 - t1) + ": " + func.__name__)
        return None

    # define a function
    # @time_test
    def execute_copy(self,dbname, user, password, host, port, input_path, table_name):
        con = psycopg2.connect(database=dbname, user=user, password=password, host=host,
                               port=port)
        cursor = con.cursor()
        directory = input_path
        for filename in os.listdir(directory):
            if filename.endswith(".csv") or filename.endswith(".txt"):
                print(str(os.path.join(directory, filename)))
                file = open(str(os.path.join(directory, filename)))
                cursor.copy_from(file, table_name, sep=",")
            else:
                continue

        con.commit()
        con.close()

    # @time_test
    def write(self,df, url, table_name, user, password, driver):
        df.write.format("jdbc"). \
            mode(saveMode="append"). \
            option("url", url). \
            option("dbtable", table_name). \
            option("user", user). \
            option("password", password). \
            option("driver", driver). \
            option("batchsize", 25000). \
            save()

    def getSparkDF(self,input_path):
        conf = SparkConfiguration().getSparkConf()
        spark = SparkSession.builder. \
            config(conf=conf). \
            appName("pyspark postgres performance test"). \
            getOrCreate()
        schema = FileSchema().empSchema()
        df = spark.read.format("csv").load(input_path, schema=schema, inferSchema=False)
        print("No of Partitions:{0}".format(df.rdd.getNumPartitions()))
        return df
        # df.repartition(10).write.option("maxRecordsPerFile", 50000).mode("overwrite").csv(r"C:\Users\91889\Desktop\Spark-Training\output")
