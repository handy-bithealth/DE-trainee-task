from pyspark.sql import SparkSession
from config import url_mysql, user_mysql, pass_mysql

Spark = SparkSession\
        .builder\
        .appName('moving data to mysql')\
        .getOrCreate()

data = Spark.read.load('covid-variants.csv',format = 'csv',header = True)

data.createOrReplaceTempView("data_table")

data_filter = Spark.sql("select * from data_table limit 100")

data_filter.write \
        .format("jdbc") \
        .option("url", url_mysql) \
        .option("dbtable", 'covid_variants') \
        .option("user", user_mysql) \
        .option("password", pass_mysql) \
        .mode("append") \
        .save()

print("FINISH")