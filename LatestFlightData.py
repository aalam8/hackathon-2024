# Databricks notebook source
# Import necessary libraries
import plotly.express as px
from pyspark.sql import SparkSession
from datetime import datetime, timezone, timedelta

# Initialize Spark session
spark = SparkSession.builder.appName("FlightDataDashboard").getOrCreate()

# Set the storage account key
storage_account_name = "travelplannerhackthon"
storage_account_access_key = dbutils.secrets.get(scope="hackathon", key="account_key")

spark.conf.set(
  "fs.azure.account.key."+storage_account_name+".dfs.core.windows.net",
  storage_account_access_key
)

# Query distinct values for origin_country and callsign
distinct_origins = spark.sql("SELECT DISTINCT origin_country FROM delta.`abfss://flights@travelplannerhackthon.dfs.core.windows.net/delta/flights`").collect()
distinct_callsigns = spark.sql("SELECT DISTINCT callsign FROM delta.`abfss://flights@travelplannerhackthon.dfs.core.windows.net/delta/flights`").collect()

# Convert to lists for dropdowns
origin_options = [row.origin_country for row in distinct_origins]

# Create dropdown widgets for source and callsign
dbutils.widgets.dropdown("src", origin_options[0], origin_options, "Select Source Country")

# Get the selected source and callsign
src = dbutils.widgets.get("src")

# SQL query to get the latest location of each flight based on the selected source and callsign
query = f"""
SELECT
  icao24,
  callsign,
  origin_country,
  longitude,
  latitude,
  baro_altitude,
  velocity,
  MAX(last_contact) as last_contact
FROM
  delta.`abfss://flights@{storage_account_name}.dfs.core.windows.net/delta/flights`
WHERE
  origin_country = '{src}'
GROUP BY
  icao24, callsign, origin_country, longitude, latitude, baro_altitude, velocity
ORDER BY
  last_contact DESC
"""

# Execute the query
df_spark = spark.sql(query)

# Convert Spark DataFrame to Pandas DataFrame for visualization
df_pd = df_spark.toPandas()
df_pd = df_pd.dropna(subset=['last_contact'])
df_pd['last_contact'] = df_pd['last_contact'].apply(lambda x: datetime.fromtimestamp(x, timezone.utc).astimezone(timezone(timedelta(hours=-8))).strftime('%Y-%m-%d %H:%M:%S'))

# Create a Plotly scatter mapbox plot
fig = px.scatter_mapbox(df_pd, lat="latitude", lon="longitude", hover_name="callsign", hover_data=["velocity", "baro_altitude", "last_contact"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=600)

fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# Show the plot
fig.show()


# COMMAND ----------

