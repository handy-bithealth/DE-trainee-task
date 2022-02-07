from pyspark.sql import SparkSession
from config import url_mysql, user_mysql, pass_mysql

Spark = SparkSession\
        .builder\
        .appName('moving data to mysql')\
        .getOrCreate()

data = Spark.read.load('exchange_rate.csv',format = 'csv',header = True)

data.write \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("dbtable", 'exchange_rate') \
        .option("user", user_mysql) \
        .option("password", pass_mysql) \
        .mode("append") \
        .save()

print("FINISH")