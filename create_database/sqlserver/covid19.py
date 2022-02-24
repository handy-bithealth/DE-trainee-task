from pyspark.sql import SparkSession
from config import url_sqlserver, user_sqlserver, pass_sqlserver

Spark = SparkSession\
        .builder\
        .appName('covid19 - movingdata')\
        .getOrCreate()

data = Spark.read.load('data_article_fix.csv',format = 'csv',header = True)

data.write \
        .format("jdbc") \
        .option("url", url_sqlserver) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option("dbtable", 'ds_articles') \
        .option("user", user_sqlserver) \
        .option("password", pass_sqlserver) \
        .mode("append") \
        .save()

print("FINISH")