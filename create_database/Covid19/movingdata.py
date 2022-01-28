import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# read data JDBC
conf = SparkConf()
conf.setAppName("movingdata")
sc = SparkContext.getOrCreate(conf=conf)
​
# Spark = SparkSession(sc)
# ​
# data1 = Spark.read.csv("covid-variants.csv")

# print(data1)

import pandas as pd

data = pd.read_csv("F:\1. Bithealth\3_Data_Engineer_Task\4_createdatabase\covid-variants.csv")


# data1.write\
#         .format("jdbc")\
#         .option("url", f"jdbc:postgresql://157.245.145.217:5432/Apple") \
#         .option("dbtable", 'appletable') \
#         .option("user", 'handy') \
#         .option("password", 'Terminalku_98') \
#         .save()
​
#write to parquet
# JdbcDF.write.parquet(f"gs://{bucket_path}/{schema_source}/{table_source}")