import findspark
findspark.init("/opt/spark-2.4.4-bin-hadoop2.6")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import year, month, col, sum, udf, substring, split, regexp_replace


#create a function to connect to spark
def spark_conf(app_name, n_cores = "*", executor_memory = 2, driver_memory = 20):
    global conf_file
    n_cores = "local[cores]".replace("cores", str(n_cores))
    executor_memory = "numg".replace("num",str(executor_memory))
    driver_memory = "numg".replace("num",str(driver_memory))
    conf_file = SparkConf().setAppName('AggData')
    conf_file = (conf_file.setMaster(n_cores)
            .set("spark.executor.memory", executor_memory)
            .set("spark.driver.memory", driver_memory))
    return(conf_file)


def start_spark(conf):
    sc = SparkContext(conf = conf_file)
    spark = SparkSession(sc)
    return(spark)


def spark_df(files, spark):
    results = {}
    if type(files) == list:
        return(map(lambda x: spark.read.csv(x,header = True,  inferSchema = True),files))
    else:
        for year, file in files.items():
            results.update({year: map(lambda x: spark.read.csv(x,header = True,  inferSchema = True),file)})
        return(results)
