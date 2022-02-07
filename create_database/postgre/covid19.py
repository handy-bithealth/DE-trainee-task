from pyspark.sql import SparkSession
from config import url_postgre, user_postgre, pass_postgre 

Spark = SparkSession\
        .builder\
        .appName('moving data to postgre')\
        .getOrCreate()

data = Spark.read.load('covid-variants.csv',format = 'csv',header = True)

data.write \
        .format("jdbc") \
        .option("url", url_postgre) \
        .option("dbtable", 'covid_variants') \
        .option("user", user_postgre) \
        .option("password", pass_postgre) \
        .mode("append") \
        .save()
