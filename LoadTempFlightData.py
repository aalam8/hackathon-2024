# Databricks notebook source
returned_table = dbutils.notebook.run("FlightPrices", 60)
storage_account_name = "travelplannerhackthon"
storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key)
from pyspark.sql import SparkSession

# Read data from Delta Lake
df_spark = spark.read.format("delta").load("abfss://flights@travelplannerhackthon.dfs.core.windows.net/delta/flights")

# Create a temporary view
df_spark.createOrReplaceTempView("flights_temp_view")

