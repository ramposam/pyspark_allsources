import configparser as cp
from pyspark import SparkConf
class PostgresConf:
    def __init__(self):
        self.config = cp.ConfigParser()
        self.config.read("connection.properties")

    def getConnectoin(self):
        props = {}
        for key,value in self.config.items("POSTGRES_CONF"):
            props[key]=value
        return props