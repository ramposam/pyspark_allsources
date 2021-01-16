import configparser as cp
from pyspark import SparkConf
class PostgresConf:
    def getConnectoin():
        config = cp.ConfigParser()
        config.read("connection.properties")
        props = {}
        for key,value in config.items("POSTGRES_CONF"):
            props[key]=value
        return props