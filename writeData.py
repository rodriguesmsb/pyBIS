from functools import reduce

def unionAll(dfs):
    return reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), dfs)

def union_all(df_spark):
    results = {}
    if type(df_spark) == map:
        return(unionAll(df_spark))
    else:
        for year in df_spark:
            results.update({year: unionAll(df_spark[year])})
        return(results)

def write_csv(df, file_name, files = False):
    if files is False:
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
            df.to_csv(file_name + str(year) + ".csv", index = False)     