# Databricks notebook source
storage_account_name = "travelplannerhackthon"
storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)
file_location = "abfss://synapse@travelplannerhackthon.dfs.core.windows.net/"
file_type = "csv"
df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
save_location = file_location + "saved/"
df.write.format("delta").mode("overwrite").save(save_location)
dbutils.notebook.exit(save_location) 

# COMMAND ----------



storage_account_name = "travelplannerhackthon"
storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)
file_location = "abfss://synapse@travelplannerhackthon.dfs.core.windows.net/"
file_type = "csv"

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

schema = StructType([
  StructField("Airline", StringType(), True),
  StructField("Date_Of_Journey", StringType(), True),
  StructField("Source", StringType(), True),
  StructField("Destination", StringType(), True),
  StructField("Route", StringType(), True),
  StructField("Dep_Time", StringType(), True),
  StructField("Arrival_Time", StringType(), True),
  StructField("Duration", StringType(), True),
  StructField("Total_Stops", StringType(), True),
  StructField("Additional_Info", StringType(), True),
  StructField("Price", IntegerType(), True)
])

df = spark.readStream.format(file_type).option("header", True).schema(schema).load(file_location)
save_location = file_location + "saved/"
query = df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "dbfs:/user/hive/warehouse/streaming_flight") \
    .table("streaming_flight")

query.awaitTermination()
# df.write.format("delta").mode("overwrite").save(save_location)
#dbutils.notebook.exit(save_location) 

# COMMAND ----------

from pyspark.sql.functions import expr

# Define Kafka broker and topic
kafka_broker = "databricktest.servicebus.windows.net:9093"
topic = "flight"

# Read stream from Kafka
streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", topic) \
    .load()

# Process stream

# Write stream to console (for testing purposes)
query = streaming_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()