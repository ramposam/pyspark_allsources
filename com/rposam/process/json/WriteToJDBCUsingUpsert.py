from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from com.rposam.config.PostgresConf import PostgresConf
from com.rposam.util.logger import Log4j

from com.rposam.config.SparkConf import SparkConfiguration
import psycopg2


def writeToPostgres(df):
    conn = psycopg2.connect("dbname='spark' user='postgres' host='127.0.0.1' password='postgres'")
    cur = conn.cursor()
    #cur.execute("DELETE FROM spark.cars")
    for r in df.toLocalIterator():
        # columns are used in conflict must be having unique index
        # if more than one column used in conflict then combination must have unique index
        query = """
                INSERT INTO spark.cars(kind,make, model, price,style) VALUES (%s, %s, %s, %s, %s)                
                ON CONFLICT (make,model)
                DO UPDATE SET price = excluded.price 
                """
        cur.execute(query, r)

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    conf = SparkConfiguration().getSparkConf()

    spark = SparkSession.builder.config(conf=conf).appName(
        "Reading cars.json and Writing to Postgresql using JDBC").getOrCreate()

    logger = Log4j(spark)
    logger.info("json file reading strted...")
    df = spark.read.format("json").load(r"json\cars.json")

    logger.info("Convert json dataframe columns to readable format...")

    logger.info("Writing output to jdbc(postgres)...")
    jdbcProps = PostgresConf().getConnectoin()
    writeToPostgres(df)
    logger.info("Writing  to jdbc(postgres) is completed ...")
