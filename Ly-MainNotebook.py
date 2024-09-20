# Databricks notebook source
#returned_table = dbutils.notebook.run("FlightPrices", 60)
#storage_account_name = "travelplannerhackthon"
#storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")
#spark.conf.set(
#  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
#  storage_account_access_key)




# COMMAND ----------

from pyspark.sql.functions import expr, base64, col
import time
import random
from pyspark.sql.types import StringType, IntegerType, StructType, StructField, TimestampType
from pyspark.sql.functions import from_json

event_hub_connection_str = dbutils.secrets.get(scope="hackathon", key="eh_conn_str")
ehConf = {
  'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_str)
}
schema = StructType([
  StructField("Airline", StringType(), True),
  StructField("Date_Of_Journey", TimestampType(), True),
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

ehConf['eventhubs.consumerGroup'] = "$Default"

df = spark \
  .readStream \
  .format("eventhubs") \
  .options(**ehConf) \
  .load()

@udf(StringType())
def get_delaytime():
    return str(random.randint(0, 60))

@udf(StringType())
def get_ontime():
    return random.choice(['Yes', 'No'])

df = df.withColumn("body", expr("CAST(body AS STRING)"))
df = df.withColumn("decoded_body", expr("CAST(base64(body) AS BINARY)"))
updated_df = df.select("body").withColumn("DelayTime", get_delaytime())

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

# Parse the body column as JSON
parsed_df = updated_df.withColumn("body", from_json(col("body"), schema))

# Dynamically select all fields from the parsed body struct
all_fields = [col(f"body.{field.name}") for field in schema.fields]
df_with_all_fields = parsed_df.select(*all_fields, "DelayTime").drop("Date_of_Journey")

df_with_all_fields = df_with_all_fields.withColumn(
    "Date_of_Journey",
    expr("date_add(current_date(), cast(round(rand() * 1) as int))")
)


# Write stream to Delta table, creating the table if it doesn't exist
# Define the database and table name
database_name = "default"
table_name = "streaming_flights"

# Write stream to Delta table in the catalog, creating the table if it doesn't exist
query = df_with_all_fields.writeStream \
    .foreachBatch(lambda batch_df, batch_id: batch_df.write \
                  .format("delta") \
                  .mode("append") \
                  .option("path", f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}") \
                  .saveAsTable(f"{database_name}.{table_name}")) \
    .option("checkpointLocation", f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}_checkpoint") \
    .start()

# Keep the query running and wait for termination
time.sleep(600)

# Stop the query when no longer needed
query.stop()

# COMMAND ----------

streaming_df = spark.readStream \
    .format("delta") \
    .table("streaming_flights")

display(streaming_df)