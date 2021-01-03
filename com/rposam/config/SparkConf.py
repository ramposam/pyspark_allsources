import configparser as cp
from pyspark import SparkConf
class SparkConfiguration:
    def getSparkConf(self):
        sparkConf = SparkConf()
        conf = cp.ConfigParser()
        conf.read(r"application.properties")
        for key,value in conf.items("SPARK_APP_CONFIGS"):
            sparkConf.set(key,value)
        return sparkConf