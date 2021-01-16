import configparser as cp
import sys
import time
from com.rposam.jdbc.PostgrecopyVSDataFrameWriter import PostgrecopyVSDataFrameWriter
if __name__ == "__main__":
    conf_path = sys.argv[1]
    input_path = sys.argv[2]
    config = cp.ConfigParser()
    config.read(conf_path)
    host = config.get("POSTGRES_CRED_PSYCOPYG", "host")
    port = config.get("POSTGRES_CRED_PSYCOPYG", "port")
    user = config.get("POSTGRES_CRED_PSYCOPYG", "user")
    password = config.get("POSTGRES_CRED_PSYCOPYG", "password")
    dbname = config.get("POSTGRES_CRED_PSYCOPYG", "dbname")
    driver = config.get("POSTGRES_CRED_PSYCOPYG", "driver")
    table_name = "spark.emp"
    # args = [(i, i + 1) for i in range(1, 1 * 10 ** 6, 2)]
    url = "jdbc:postgresql://{}:{}/{}".format(host, port, dbname)
    print(url)

    df = PostgrecopyVSDataFrameWriter().getSparkDF(input_path)
    t1 = time.time()
    PostgrecopyVSDataFrameWriter().write(df, url, table_name, user, password, driver)
    t2 = time.time()
    print(str(t2 - t1) + ": write")
    t1 = time.time()
    PostgrecopyVSDataFrameWriter().execute_copy(dbname, user, password, host, port, input_path, table_name)
    t2 = time.time()
    print(str(t2 - t1) + ": execute_copy")
