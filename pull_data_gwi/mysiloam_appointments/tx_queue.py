# import library
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
​
import additional
from additional import config

# database information
database_source = config["DB_SOURCE"]["DB_NAME"]
schema_source = "public"
table_source = "tx_queue"
user_source = config["DB_SOURCE"]["DB_USER"]
password_source = config["DB_SOURCE"]["DB_PWD"]
database_host = config["DB_SOURCE"]["DB_HOST"]
database_port = config["DB_SOURCE"]["DB_PORT"]
bucket_path = config["DB_DESTINATION"]["BUCKET_PATH"]

# read data JDBC
conf = SparkConf()
conf.setAppName(f"{table_source}")
sc = SparkContext.getOrCreate(conf=conf)

Spark = SparkSession(sc)

JdbcDF = Spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{database_host}:{database_port}/{database_source}") \
        .option("dbtable", table_source) \
        .option("user", user_source) \
        .option("password", password_source) \
        .load()

#write to parquet
JdbcDF.write.parquet(f"gs://{bucket_path}/{schema_source}/{table_source}")