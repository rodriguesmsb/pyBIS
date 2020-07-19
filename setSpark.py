import findspark
import sys
import os
from os import system
findspark.init("spark-3.0.0-bin-hadoop3.2")

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import year, month, col, sum, udf,\
    substring, split, regexp_replace

system('export SPARK_HOME=spark-3.0.0-bin-hadoop3.2')
system('export PATH=$SPARK_HOME/bin')
system('export PYSPARK_PYTHON=python3')

#create a function to connect to spark
def spark_conf(n_cores, executor_memory, \
               driver_memory = 20):

    global conf_file

    n_cores = "local[{}]".format(str(n_cores))

    executor_memory = "numg".replace("num",str(executor_memory))
    driver_memory = "numg".replace("num",str(driver_memory))

    conf_file = SparkConf().setAppName('AggData')
    conf_file = (conf_file.setMaster(n_cores)
                 .set('spark.executor.memory', executor_memory)
                 .set('spark.driver.memory', driver_memory))

    return (conf_file)

def start_spark(conf):
    sc = SparkContext(conf = conf_file)
    spark = SparkSession(sc)
    return (spark)


def spark_df(files, spark):
    results = {}
    if type(files) == list:
        return(map(lambda x: spark.read.csv(x,header = True,\
                                            inferSchema = True),files))
    else:
        for year, file in files.items():
            results.update({year: map(lambda x: spark.read.csv(x,header = True,  inferSchema = True),file)})
        return(results)
