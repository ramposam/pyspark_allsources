import configparser as cp
from pyspark import SparkConf
class HiveConfiguration:
    def getConnectoin():
        config = cp.ConfigParser()
        config.read("hive.properties")
        props = {}
        for key,value in config.items("HIVE_LOCAL_CONF"):
            props[key]=value
        return props