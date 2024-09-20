# Databricks notebook source
# MAGIC %sql
# MAGIC select * from default.streaming_flights

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, expr
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf

database_name = "default"
table_name = "streaming_flights"
data = spark.read.format("delta").load(f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}")

user_preference = StringIndexer(inputCol="Airline", outputCol="user_preference")
routeidx = StringIndexer(inputCol="Route", outputCol="route_index")

data = data.withColumn(
    "total_minutes",
    expr("regexp_extract(Duration, '([0-9]+)h', 1) * 60 + regexp_extract(Duration, '([0-9]+)m', 1)")
)
data = data.withColumn(
    "userId",
    (F.rand() * 100).cast("int")
)

# Fit and transform the indexers
data = user_preference.fit(data).transform(data)
data = routeidx.fit(data).transform(data)
data = data.withColumn("Price", data["Price"].cast("int"))

def calculate_rating(price, duration, delay_time, airline):
    normalized_price = 1 / (price + 1)  # Inverse of price
    normalized_duration = 1 / (duration + 1)  # Inverse of duration
    return 0.3 * delay_time + 0.5 * normalized_price + 0.1 * normalized_duration + 0.1 * airline

# Cast total_minutes to integer
data = data.withColumn("total_minutes", col("total_minutes").cast("integer"))
data = data.withColumn("Price", col("Price").cast("integer"))
data = data.withColumn("source_des", col("Source"))
source_des_idx = StringIndexer(inputCol="Source", outputCol="source_des_idx")
data = source_des_idx.fit(data).transform(data)

rating_udf = udf(calculate_rating, FloatType())
df_with_route_index = data.withColumn(
    "rating", rating_udf(data["Price"], data["total_minutes"], data["Airline"], data["DelayTime"])
)
df_with_route_index.show(1, truncate=False)

# Initialize ALS model
als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="route_index", ratingCol="rating", coldStartStrategy="drop")

# Train the ALS model on the current batch of streaming data
model = als.fit(df_with_route_index)

# Generate recommendations for all users (or save the model for later use)
user_recs = model.recommendForAllUsers(10)

data = data.drop("route_index", "source_des_idx")
converter = IndexToString(inputCol="ridx", outputCol="route",labels=routeidx.fit(data).labels)
converter_source = IndexToString(inputCol="source_des_idx", outputCol="source_des",labels=source_des_idx.fit(data).labels)
from pyspark.sql.functions import explode
exploded_recs = user_recs.withColumn("recommendation", explode("recommendations"))
exploded_recs = exploded_recs.withColumn("ridx", col("recommendation.route_index"))
final_recs = converter.transform(exploded_recs)
final_recs.show(1, truncate=False)
final_recs = converter_source.transform(final_recs).select("source_des","route")
final_recs.write.format("delta").mode("overwrite").saveAsTable('recommendation')

display(final_recs)

# COMMAND ----------

import plotly.graph_objs as go
from pyspark.sql import SparkSession
import pandas as pd
from IPython.display import display

database_name = "default"
table_name = "streaming_flights"

# Create a Plotly FigureWidget
fig = go.FigureWidget()

# Initialize an empty line plot
trace = fig.add_bar(x=[], y=[])
display(fig)  # Display the Plotly figure

# Function to update the figure with new batch data
def update_plotly(batch_df, batch_id):
    pandas_df = batch_df.toPandas()  # Convert Spark DataFrame to Pandas
    new_data_x = pandas_df['Airline']  # Assume a 'timestamp' column exists
    new_data_y = pandas_df['DelayTime']  # Delay time column

    # Append new data to the existing plot
    trace = fig.data[0]
    trace.x = trace.x + tuple(new_data_x)
    trace.y = trace.y + tuple(new_data_y)

# Read streaming data (e.g., from Delta Lake or Kafka)
streaming_df = spark.readStream.format("delta").load(f"dbfs:/user/hive/warehouse/{database_name}.db/{table_name}")

# Write the streaming data in batches, and update the plot for each batch
query = streaming_df.writeStream.foreachBatch(update_plotly).start()

# Wait for the streaming to finish
query.awaitTermination()

# COMMAND ----------

streaming_df = spark.readStream \
    .format("delta") \
    .table("streaming_flights")

display(streaming_df)