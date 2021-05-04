#!/usr/bin/env python3
# -*- coding:utf-8 -*-

from os import system, path, environ
import platform
import findspark
spark = path.join(path.dirname(__file__), 'spark-3.0.0-bin-hadoop3.2')
findspark.init(spark)
if platform.system().lower() == 'linux':
    jdk = path.join(path.dirname(__file__), 'java-15-linux')
    environ['JAVA_HOME'] = jdk
    environ['SPARK_HOME'] = spark
    environ['PYSPARK_PYTHON'] = 'python3'
elif platform.system().lower() == 'windows':
    jdk = path.join(path.dirname(__file__), 'java-15-windows')
    environ['JAVA_HOME'] = jdk
    environ['SPARK_HOME'] = spark
    environ['HADOOP_HOME'] = spark
    environ['PYSPARK_PYTHON'] = 'py'

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import (year, month, col, sum, udf, substring,
                                   split, regexp_replace)


def memory():
    if platform.system().lower() == 'linux':
        with open('/proc/meminfo', 'r') as mem:
            ret = {}
            tmp = 0
            for i in mem:
                sline = i.split()
                if str(sline[0]) == 'MemTotal:':
                    ret['total'] = int(sline[1])
                elif str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
                    tmp += int(sline[1])
            ret['free'] = tmp
            ret['used'] = int(ret['total']) - int(ret['free'])
        return round(int(ret['total'] / 1024) / 1000)

    elif platform.system().lower() == 'windows':
        return 4


def spark_conf(app_name, n_cores="*", executor_memory=2,
               driver_memory=20):

    global conf_file
    n_cores = "local[cores]".replace("cores", str(n_cores))
    executor_memory = "numg".replace("num", str(executor_memory))
    driver_memory = "numg".replace("num", str(driver_memory))
    conf_file = SparkConf().setAppName('pyBIS')
    conf_file = (conf_file.setMaster(n_cores)
                 .set("spark.executor.memory", executor_memory)
                 .set("spark.driver.memory", driver_memory))
    return(conf_file)


def start_spark(conf):
   try:
       sc = SparkContext(conf=conf_file)
       spark = SparkSession(sc)
       # spark.debug.maxToStringFields(100)
   except ValueError:
       sc = SparkContext.getOrCreate()
   spark = SparkSession(sc)
   return(spark)


def spark_df(files, spark):
    results = {}
    if type(files) == list:
        return (map(lambda x: spark.read.csv(x, header=True,
                                             inferSchema=True), files))
    else:
        for year, file in files.items():
            results.update({year: map(
                lambda x: spark.read.csv(x, header=True,
                                         inferSchema=True), file)})
        return(results)
