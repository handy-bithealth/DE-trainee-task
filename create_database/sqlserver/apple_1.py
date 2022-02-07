from pyspark.sql import SparkSession
from config import url_sqlserver, user_sqlserver, pass_sqlserver

Spark = SparkSession\
        .builder\
        .appName('moving data to sql server')\
        .getOrCreate()

data = Spark.read.load('apple_product_price_list.csv',format = 'csv',header = True)

data.write \
        .format("jdbc") \
        .option("url", url_sqlserver) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", 'apple_price_list') \
        .option("user", user_sqlserver) \
        .option("password", pass_sqlserver) \
        .mode("append") \
        .save()

print("FINISH")