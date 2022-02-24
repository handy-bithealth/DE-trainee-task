import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession

# # read data JDBC
# conf = SparkConf()
# conf.setAppName("movingdata")
# sc = SparkContext.getOrCreate(conf=conf)
# ​
# Spark = SparkSession(sc)
# ​
# # data1 = Spark.read.csv("F:\1. Bithealth\3_Data_Engineer_Task\4_createdatabase\Apple\exchange rate.csv")

import pandas as pd

data1 = pd.read_csv("F:\1. Bithealth\3_Data_Engineer_Task\4_createdatabase\Article\medium-data-science-articles-2021.csv")


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