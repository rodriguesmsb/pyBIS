import operator

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

def merge_data(data, vars):
    results = {}
    if type(df_spark) == map:
        df_spark = list(df_spark)
        return(map(lambda x: x.filter(condition), df_spark))
    else:
        for year in df_spark:
            results.update({year:map(lambda x: x.filter(condition), list(df_spark[year]))})
        return(results)


##### Insert more functionality using spark