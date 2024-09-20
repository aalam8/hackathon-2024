# Databricks notebook source
import random
from pyspark.sql.functions import col, lit, current_timestamp

# Change prices by a random amount of 10 and update updatedTime to current timestamp
# Correctly cast the result back to decimal(10,2) to maintain the column type
before = spark.table("flight_prices_dh")
display(before)

updated_df = spark.table("flight_prices_dh") \
.withColumn("price", (col("price") + lit(random.randint(-15, 50))).cast("decimal(10,2)")) \
    .withColumn("updatedTime", current_timestamp())

# Overwrite the table with updated prices
updated_df.write.format("delta").mode("overwrite").saveAsTable("flight_prices_dh")

# Print table
display(updated_df)