import time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql import SparkSession
import psycopg2
import os
import sys
import configparser as cp
from com.rposam.config.SparkConf import SparkConfiguration

class psycopg2VsSparkDF:
    schema = StructType([
            StructField("empno", IntegerType()),
            StructField("ename", StringType()),
            StructField("job", StringType()),
            StructField("mgr", IntegerType()),
            StructField("hiredate", DateType()),
            StructField("sal", DoubleType()),
            StructField("comm", IntegerType()),
            StructField("deptno", IntegerType())
        ])

    def time_test(func):
        t1 = time.time()
        func()
        t2 = time.time()
        print("==============================================================")
        print(str(t2 - t1) + ": " + func.__name__)
        return None

    # define a function
    # @time_test
    def execute_copy(input_path,table_name):
        con = psycopg2.connect(database="spark", user="postgres", password="postgres", host="127.0.0.1",
                               port=5432)
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
    def write(df):
        df.write.format("jdbc"). \
            mode(saveMode="append"). \
            option("url", "jdbc:postgresql://localhost:5432/spark"). \
            option("dbtable", "spark.emp"). \
            option("user", "postgres"). \
            option("password", "postgres"). \
            option("driver", "org.postgresql.Driver"). \
            option("batchsize", 25000). \
            save()

    def getSparkDF(input_path):
        conf = SparkConfiguration().getSparkConf()
        spark = SparkSession.builder. \
            config(conf).appName("Spark vs Postgresql performance test"). \
            getOrCreate()

        df = spark.read.format("csv").schema(schema=psycopg2VsSparkDF.schema).option("header","false").load(input_path)
        print("No of Partitions:{0}".format(df.rdd.getNumPartitions()))
        return df
        # df.repartition(10).write.option("maxRecordsPerFile", 50000).mode("overwrite").csv(r"C:\Users\91889\Desktop\Spark-Training\output")
    if __name__ == "__main__":
        conf_path = sys.argv[1]
        input_path = sys.argv[2]

        df = getSparkDF(input_path)
        t1 = time.time()
        write(df)
        t2 = time.time()
        print(str(t2 - t1) + ": write")
        t1 = time.time()
        execute_copy(input_path,"spark.emp")
        t2 = time.time()
        print(str(t2 - t1) + ": execute_copy")
