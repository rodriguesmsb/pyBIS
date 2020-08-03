#!/usr/bin/env python3

from os import system, path
import findspark

findspark.init(f'{path.os.getcwd()}/spark-3.0.0-bin-hadoop3.2/')

system(f'export SPARK_HOME={path.os.getcwd()}/spark-3.0.0-bin-hadoop3.2/')
system('export PATH=$SPARK_HOME/bin')
system('export PYSPARK_PYTHON=python3')
system(f'export PATH=$PATH:{path.os.getcwd()}/spark-3.0.0-bin-hadoop3.2/')

import platform
from simpledbf import Dbf5
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import (year, month, col, sum, udf, substring,
                                   split, regexp_replace)

def memory():
    with open('/proc/meminfo','r') as mem:
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
    return round(int(ret['total']/1024)/1000)

def spark_conf(app_name, n_cores = "*", executor_memory = 2,
               driver_memory = 20):

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
    global spark
    sc = SparkContext(conf = conf_file)
    spark = SparkSession(sc)
    return(spark)

def spark_df(files, spark):
    results = {}
    if type(files) == list:
        return (map(lambda x: spark.read.csv(x, header = True,
                                             inferSchema = True), files))
    else:
        for year, file in files.items():
            results.update({year: map(
                lambda x: spark.read.csv(x, header = True,
                                         inferSchema = True), file)})
        return(results)

def spark_read(widget):
    tmp = path.expanduser('/tmp/')
    doc_db = path.expanduser('~/Documentos/files_db/')

    linux = [platform.system().lower() == 'linux', 
            widget.currentText() != 'SELECT DATA']
    if all(linux):
        dbf = widget.currentText().split('/')[1].split('.')[0] + '.dbf'
        system(f'blast-dbf {doc_db}{widget.currentText()} {tmp}{dbf}')
        Dbf5(f'{tmp}/{dbf}').to_csv(tmp + 'teste.csv')
        df = spark.read.csv(f'{tmp}teste.csv', header=True)
        print(df.show())
    else:
        pass
