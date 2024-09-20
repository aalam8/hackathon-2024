# Databricks notebook source
# Add new version of data in Azure Delta Lake
new_save_location = file_location + "saved_v2/"
df.write.format("delta").mode("overwrite").save(new_save_location)

# Return both versions of data
df_versions = spark.read.format("delta").load(save_location).union(spark.read.format("delta").load(new_save_location))

# Display both versions of data
display(df_versions)

# COMMAND ----------

# DBTITLE 1,drop table
# spark.sql("DROP TABLE IF EXISTS flight_prices_dh")

# COMMAND ----------

# DBTITLE 1,Initial data set
# MAGIC %sql
# MAGIC
# MAGIC -- Creating Delta Lake table for flights
# MAGIC CREATE TABLE IF NOT EXISTS flight_prices_dh (updatedTime TIMESTAMP, flight_departure STRING, source STRING, destination STRING, price DECIMAL(10,2));
# MAGIC
# MAGIC INSERT INTO flight_prices_dh (updatedTime, flight_departure, source, destination, price) 
# MAGIC VALUES 
# MAGIC (now(), "2024-09-19T12:00:00", 'SEA', 'TYO', 123.45)
# MAGIC     (now(), "2024-09-20T08:00:00", 'LAX', 'LHR', 456.78),
# MAGIC     (now(), "2024-09-21T09:30:00", 'NYC', 'PAR', 789.01),
# MAGIC     (now(), "2024-09-22T07:45:00", 'SFO', 'BER', 234.56),
# MAGIC     (now(), "2024-09-23T10:15:00", 'MIA', 'MAD', 567.89),
# MAGIC     (now(), "2024-09-24T11:00:00", 'CHI', 'ROM', 890.12),
# MAGIC     (now(), "2024-09-25T05:30:00", 'SEA', 'DEL', 345.67),
# MAGIC     (now(), "2024-09-26T06:45:00", 'LAX', 'SYD', 678.90),
# MAGIC     (now(), "2024-09-28T04:00:00", 'SFO', 'BKK', 123.34),
# MAGIC     (now(), "2024-09-29T03:00:00", 'SEA', 'LAX', 234.56),
# MAGIC     (now(), "2024-09-30T02:15:00", 'SEA', 'NYC', 345.67),
# MAGIC     (now(), "2024-10-01T01:30:00", 'SEA', 'SFO', 456.78),
# MAGIC     (now(), "2024-10-02T05:45:00", 'SEA', 'MIA', 567.89),
# MAGIC     (now(), "2024-10-03T06:00:00", 'SEA', 'CHI', 678.90),
# MAGIC     (now(), "2024-10-04T07:15:00", 'LAX', 'NYC', 789.01),
# MAGIC     (now(), "2024-10-05T08:30:00", 'LAX', 'SFO', 890.12),
# MAGIC     (now(), "2024-10-06T09:45:00", 'LAX', 'MIA', 901.23),
# MAGIC     (now(), "2024-10-07T10:00:00", 'LAX', 'CHI', 123.45),
# MAGIC     (now(), "2024-10-08T11:15:00", 'NYC', 'SFO', 234.56),
# MAGIC     (now(), "2024-10-09T12:30:00", 'NYC', 'MIA', 345.67),
# MAGIC     (now(), "2024-10-10T13:45:00", 'NYC', 'CHI', 456.78),
# MAGIC     (now(), "2024-10-11T14:00:00", 'SFO', 'MIA', 567.89),
# MAGIC     (now(), "2024-10-12T15:15:00", 'SFO', 'CHI', 678.90),
# MAGIC     (now(), "2024-10-13T16:30:00", 'MIA', 'CHI', 789.01);
# MAGIC
# MAGIC SELECT * FROM flight_prices_dh;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE flight_prices_dh SET price = '234' WHERE source = "SEA" and destination = "TYO";

# COMMAND ----------

# DBTITLE 1,Describe the historic changes to the table
# MAGIC %sql
# MAGIC DESCRIBE HISTORY flight_prices_dh

# COMMAND ----------

# DBTITLE 1,Get a past version
# MAGIC %sql
# MAGIC SELECT * FROM flight_prices_dh TIMESTAMP AS OF '2024-09-19T15:25:18.000Z';

# COMMAND ----------

# DBTITLE 1,Generate price changes
import time
from pyspark.sql.functions import col, lit, current_timestamp

# Assuming flight_prices_dh is the table to be updated
for i in range(10):
    # Change prices by a random amount of 10 and update updatedTime to current timestamp
    # Correctly cast the result back to decimal(10,2) to maintain the column type
    updated_df = spark.table("flight_prices_dh") \
        .withColumn("price", (col("price") + lit(10)).cast("decimal(10,2)")) \
        .withColumn("updatedTime", current_timestamp())
    # display(updated_df)

    updated_df.printSchema()
    # Overwrite the table with updated prices
    updated_df.write.format("delta").mode("overwrite").saveAsTable("flight_prices_dh")

    # Print the updated table
    display(updated_df)
    
    # Wait for 1 minute
    time.sleep(30)

# Read the updated table
# updated_df = spark.read.format("delta").load(save_location)

# Print table
display(updated_df)

# COMMAND ----------

# Import necessary libraries
import plotly.express as px
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


# Get current time as epoch seconds
current_time_epoch_seconds = int(time.time())

default_src = dbutils.widgets.get("src")

# populate drop down from latest data
distinct_src = spark.sql("SELECT DISTINCT source FROM flight_prices_dh").collect()
distinct_dest = spark.sql("SELECT DISTINCT destination FROM flight_prices_dh WHERE source = '{}'".format(default_src)).collect()
available_dates = spark.sql("SELECT DISTINCT flight_departure FROM flight_prices_dh WHERE source = '{}' AND destination = '{}'".format(default_src, dbutils.widgets.get("dest"))).collect()

# Convert to lists for dropdowns
origin_list = [row.source for row in distinct_src]
dest_list = [row.destination for row in distinct_dest]
available_dates = [row.flight_departure for row in available_dates]

# Create dropdown widgets for source and callsign
dbutils.widgets.removeAll()

dbutils.widgets.dropdown("src", origin_list[0], origin_list, "Select Source Country")
dbutils.widgets.dropdown("dest", dest_list[0], dest_list, "Select Destination Country")
dbutils.widgets.dropdown("date", available_dates[0], available_dates, "Select Date")


# Retrieve data for different time frames without collecting to driver
current_df = spark.sql(f"""
                       SELECT *, 'Current' AS time_frame FROM flight_prices_dh 
                       WHERE source = '{dbutils.widgets.get("src")}' AND destination = '{dbutils.widgets.get("dest")}'""")

all_data_df = current_df

# iterate up to a given number of hours to look back 

time_frames = [1,2,3,4,6,8,10,12,14,16,18,20]

for hours_ago in time_frames:
    df = spark.sql(f"""
        SELECT *, '{hours_ago}hrs Ago' AS time_frame 
        FROM flight_prices_dh TIMESTAMP AS OF timestamp_seconds({current_time_epoch_seconds - 60*60*hours_ago}) 
        WHERE source = '{dbutils.widgets.get("src")}' AND destination = '{dbutils.widgets.get("dest")}'
    """)
    all_data_df = all_data_df.unionByName(df)

# Convert to Pandas DataFrame for plotting
comparison_pd = all_data_df.toPandas()
display(comparison_pd)

# Create a comparison graph with Plotly
fig = px.bar(comparison_pd, x='updatedTime', y='price',
             title=f"Price Trends for {dbutils.widgets.get('src')} to {dbutils.widgets.get('dest')}: Current vs 24hrs ago",
             labels={'price':'Price', 'updatedTime':'Time', 'time_frame':'Data Point'},
             color='time_frame', barmode='group')

# Show the plot
fig.show()

fig_trend = px.scatter(comparison_pd, x='updatedTime', y='price',
                 title=f"Price Trends for {dbutils.widgets.get('src')} to {dbutils.widgets.get('dest')}: Current vs 24hrs ago",
                 labels={'price':'Price', 'updatedTime':'Time', 'time_frame':'Data Point'},
                 color='time_frame', trendline="ols")
fig_trend.show()