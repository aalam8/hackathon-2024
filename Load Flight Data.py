# Databricks notebook source
storage_account_name = "travelplannerhackthon"
storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")
spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)

container_name= "flights"

import requests
import pandas as pd
from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("FlightData").getOrCreate()

# API endpoint
api_url = "https://opensky-network.org/api/states/all"

# Request data
response = requests.get(api_url)
data = response.json()

# Convert to pandas DataFrame
columns = ["icao24", "callsign", "origin_country", "time_position", "last_contact", "longitude", "latitude", "baro_altitude", "on_ground", "velocity", "true_track", "vertical_rate", "sensors", "geo_altitude", "squawk", "spi", "position_source"]
df_pd = pd.DataFrame(data['states'], columns=columns)

# Convert pandas DataFrame to Spark DataFrame
df_spark = spark.createDataFrame(df_pd)

# Show the Spark DataFrame to verify
df_spark.show()

# Write the DataFrame to Delta Lake
df_spark.write.format("delta").mode("append").save(f"abfss://{container_name}@travelplannerhackthon.dfs.core.windows.net/delta/flights")

