#load libriries used during the  process
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import DateType, StringType
from pyspark.sql.functions import year, month, col, sum, udf, substring, split, regexp_replace
import glob as gb
import re
import operator
from functools import reduce
from tqdm import tqdm

#create a function to connect to spark
# def spark_conf(app_name, n_cores = "*", executor_memory = None, driver_memory = None):
#     n_cores = "local[cores]".replace("cores", str(n_cores))
#     executor_memory = "numGB".replace("num",str(executor_memory))
#     driver_memory = "numGB".replace("num",str(driver_memory))
#     conf_file = SparkConf().setAppName(app_name)
#     conf_file = (conf_file.setMaster(n_cores)
#             .set("spark.executor.memory", executor_memory)
#             .set("spark.driver.memory", driver_memory))
#     return(conf_file)

# def start_spark(conf_file):
#     sc = SparkContext(conf = conf_file)
#     spark = SparkSession(sc)
#     return(spark)

#Start a conf file
conf = SparkConf().setAppName("SecSUS")

#Given the parameter that will be used for the new context
conf = (conf.setMaster("local[*]")
       .set("spark.executor.memory", "2GB")
       .set("spark.driver.memory", "20GB"))

#Initialize the new context
sc = SparkContext(conf = conf)
spark = SparkSession(sc)

#Check status
spark


#Define function to point files
def select_files(path, pattern = False, state = False, years = False, extension = ".csv"):
    """
        This function is used to create a list of datasets based
    """
    results = {}
    try:
        for year in years:
            if pattern:
                files = gb.glob(path + pattern + "*" + str(year) + "*" + extension)
            else:
                files = gb.glob(path + "*" + extension)
            results.update({year:files})
        return(results)
    except:
        if pattern:
                files = gb.glob(path + pattern + "*" + extension)
        else:
            files = gb.glob(path + "*" + extension)
    return(files)

def unionAll(dfs):
    return reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)
        
def spark_df(files):
    results = {}
    if type(files) == list:
        return(map(lambda x: spark.read.csv(x,header = True,  inferSchema = True),files))
    else:
        for year, file in files.items():
            results.update({year: map(lambda x: spark.read.csv(x,header = True,  inferSchema = True),file)})
        return(results)

def column_selection(df_spark,columns = []):
    columns =[column.lower() for column in columns]
    results = {}
    if type(df_spark) == map:
        df_spark = list(df_spark)
        return(map(lambda x: x.select(columns), df_spark))
    else:
        for year in df_spark:
            results.update({year:map(lambda x: x.select(columns), list(df_spark[year]))})
        return(results)

def rows_selection(df_spark, condition):
    results = {}
    if type(df_spark) == map:
        df_spark = list(df_spark)
        return(map(lambda x: x.filter(condition), df_spark))
    else:
        for year in df_spark:
            results.update({year:map(lambda x: x.filter(condition), list(df_spark[year]))})
        return(results)

def union_all(df_spark):
    results = {}
    if type(df_spark) == map:
        return(unionAll(df_spark))
    else:
        for year in df_spark:
            results.update({year: unionAll(df_spark[year])})
        return(results)

def write_csv(df, file_name, files = None):
    if files is None:
        if len(df) == 1:
            df = df.toPandas()
            df.to_csv(file_name + ".csv", index = False)
        else:
            dfs = [data for year, data in df.items()]
            dfs = unionAll(dfs)
            dfs = dfs.toPandas()
            dfs.to_csv(file_name + ".csv", index = False)
    else:
        for year, data in df.items():
            df = data.toPandas()
            df.to_csv(str(year) + file_name + ".csv", index = False)     