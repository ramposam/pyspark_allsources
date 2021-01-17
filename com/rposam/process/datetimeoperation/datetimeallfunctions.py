from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, to_timestamp, date_add

from com.rposam.config.SparkConf import SparkConfiguration
from com.rposam.util.logger import Log4j
# Jupyternotebook set export variables

# import os
# os.environ["PYSPARK_PYTHON"] = "python3"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "jupyter"
# os.environ["PYSPARK_DRIVER_PYTHON_OPTS"]="notebook"


if __name__ == "__main__":
    conf = SparkConfiguration.getSparkConf()
    warehouseLocation = "hdfs://localhost:8020/user/hive/warehouse/sparkdb.db"

    spark = SparkSession.builder. \
        config(conf=conf). \
        config("spark.sql.warehouse.dir", warehouseLocation). \
        config("hive.metastore.uris", "thrift://localhost:9083"). \
        enableHiveSupport(). \
        appName("Date time excercises"). \
        getOrCreate()

    logger = Log4j(spark)
    # date_format arguments
    # y -> 2020,20
    # M/L -> 7; 07; Jul; July
    # D -> day-of-year	189
    # d -> day-of-month	28
    # h -> clock-hour-of-am-pm (1-12)
    # H -> hour-of-day (0-23)
    # Q/q -> quarter-of-year 3; 03; Q3; 3rd quarter
    # m	-> minute-of-hour	30
    # s	-> second-of-minute	55
    # S -> fraction-of-second	978
    # z	-> time-zone name	Pacific Standard Time; PST
    # EEEE -> Monday,Tuesday,etc..
    # MMMM -> January,Febraury,etc..
    # a -> am/pm

    emp = [(1, "AAA", "dept1", 1000, "2019-02-01 15:12:13"),
           (2, "BBB", "dept1", 1100, "2018-04-01 5:12:3"),
           (3, "CCC", "dept1", 3000, "2017-06-05 1:2:13"),
           (4, "DDD", "dept1", 1500, "2019-08-10 10:52:53"),
           (5, "EEE", "dept2", 8000, "2016-01-11 5:52:43"),
           (6, "FFF", "dept2", 7200, "2015-04-14 19:32:33"),
           (7, "GGG", "dept3", 7100, "2019-02-21 15:42:43"),
           (8, "HHH", "dept3", 3700, "2016-09-25 15:32:33"),
           (9, "III", "dept3", 4500, "2017-10-15 15:22:23"),
           (10, "JJJ", "dept5", 3400, "2018-12-17 15:14:17")]

    logger.info("Creating dataframe using list")
    empdf = spark.createDataFrame(emp, ["id", "name", "dept", "salary", "date"])

    logger.info("writing to hive database sparkdb")

    empdf.createOrReplaceTempView("emp")

    spark.sql("""
     select 
        tab.*,
        date_format(date_time,'MMMM') Month,
        date_format(date_time,'EEEE') day_of_month,
        date_add(date_time,10) as add_10days,
        date_format(date_time, 'd/M/y H:m:s') as date_to_string,
        from_unixtime(unix_timestamp) as unix_time_to_str,
        datediff(tab.system_date_time,tab.date_time) days_diff,
        system_date_time - (INTERVAL 24 HOURS) as subtract_24hrs,
        day(system_date_time) day_,
        hour(system_date_time) as extract_hr_from_date,
        minute(system_date_time) as extract_minute,
        date(system_date_time) extract_date,
        dayofweek(system_date_time) day_of_week,
        dayofmonth(system_date_time) day_of_month,
        dayofyear(system_date_time) day_of_year,
        unix_timestamp(date_time) as convert_datetime_to_unixtime,
        months_between(system_date_time,date_time) no_of_months,
        add_months(date_time,65*12) as 65_birth_date,
        last_day(date_time) as last_day_of_month,
        next_day(date_time,'WED') AS next_wednesday
        

    from (
    select e.*,
        current_timestamp() system_date_time,
        current_date as system_date,
        to_timestamp(e.date,'y-M-d H:m:s') as date_time ,
        unix_timestamp() unix_timestamp,
        to_date('2020-12-31','y-M-d') str_to_date
    from emp e) tab

    """).write.saveAsTable("sparkdb.emp")

    spark.stop()
