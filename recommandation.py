# Databricks notebook source
tables_to_drop = ["recommandation","recommandations"]
database_name = "default"

for table in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {database_name}.{table}")

# COMMAND ----------

from pyspark.ml.recommendation import ALS
from pyspark.sql.functions import col, expr
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.sql.types import FloatType
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

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

def calculate_rating(price, duration, user_preference):
    normalized_price = 1 / (price + 1)  # Inverse of price
    normalized_duration = 1 / (duration + 1)  # Inverse of duration
    return 0.7 * user_preference + 0.2 * normalized_price + 0.1 * normalized_duration

# Cast total_minutes to integer
data = data.withColumn("total_minutes", col("total_minutes").cast("integer"))
data = data.withColumn("Price", col("Price").cast("integer"))
data = data.withColumn("source_des", col("Source"))
source_des_idx = StringIndexer(inputCol="Source", outputCol="source_des_idx")
data = source_des_idx.fit(data).transform(data)

rating_udf = udf(calculate_rating, FloatType())
df_with_route_index = data.withColumn(
    "rating", rating_udf(data["source_des_idx"], data["total_minutes"], data["user_preference"])
)

# Initialize ALS model
als = ALS(maxIter=10, regParam=0.01, userCol="userId", itemCol="route_index", ratingCol="rating", coldStartStrategy="drop")

# Train the ALS model on the current batch of streaming data
model = als.fit(df_with_route_index)

# Generate recommendations for all users (or save the model for later use)
user_recs = model.recommendForAllUsers(10)

data = data.drop("route_index","source_des_idx")
converter = IndexToString(inputCol="ridx", outputCol="route",labels=routeidx.fit(data).labels)
converter_source = IndexToString(inputCol="source_des_idx", outputCol="source_des",labels=source_des_idx.fit(data).labels)
from pyspark.sql.functions import explode
exploded_recs = user_recs.withColumn("recommendation", explode("recommendations"))
exploded_recs = exploded_recs.withColumn("ridx", col("recommendation.route_index"))
final_recs = converter.transform(exploded_recs)
final_recs.show(1, truncate=False)
data_with_price_and_id = data.select("userId","Airline","Price")
join_recs = final_recs.join(data_with_price_and_id, on="userId")
#final_recs = converter_source.transform(final_recs).select("source_des","route")

def price_range(price):
    lower_bound = (price // 500) * 500
    upper_bound = lower_bound + 500
    return f"{lower_bound}~{upper_bound}"

# Register the UDF in PySpark
price_range_udf = udf(price_range, StringType())
final_recs_with_range = join_recs.withColumn("price_range", price_range_udf(join_recs["Price"]))

final_recs_with_range.show(1, truncate=False)
final_recs_with_range.write.format("delta").mode("overwrite").saveAsTable('recommendations_2')

display(final_recs_with_range)