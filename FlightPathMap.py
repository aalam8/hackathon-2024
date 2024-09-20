# Databricks notebook source
# Import necessary libraries
import plotly.express as px
import plotly.graph_objects as go
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

# Query distinct values for origin_country
distinct_origins = spark.sql("SELECT DISTINCT origin_country FROM delta.`abfss://flights@travelplannerhackthon.dfs.core.windows.net/delta/flights`").collect()

# Convert to list for dropdown
origin_options = [row.origin_country for row in distinct_origins]

# Get top 50 callsigns with the highest number of entries
top_callsigns = spark.sql("""
SELECT callsign, COUNT(*) as count
FROM delta.`abfss://flights@travelplannerhackthon.dfs.core.windows.net/delta/flights`
GROUP BY callsign
ORDER BY count DESC
LIMIT 50
""").collect()

# Convert to list for dropdown
callsign_options = [str(row.callsign) for row in top_callsigns]

# Create dropdown widgets for source and callsign
dbutils.widgets.dropdown("src", origin_options[0], origin_options, "Select Source Country")
dbutils.widgets.dropdown("callsign", callsign_options[0], callsign_options, "Select Callsign")

# Get the selected source and callsign
src = dbutils.widgets.get("src")
callsign = dbutils.widgets.get("callsign")

# SQL query to get the history of the selected flight
query = f"""
SELECT
  icao24,
  callsign,
  origin_country,
  time_position,
  longitude,
  latitude,
  baro_altitude,
  velocity,
  last_contact
FROM
  delta.`abfss://flights@{storage_account_name}.dfs.core.windows.net/delta/flights`
WHERE
  origin_country = '{src}' AND callsign = '{callsign}'
ORDER BY
  last_contact DESC
"""

# Execute the query
df_spark = spark.sql(query)

# Convert Spark DataFrame to Pandas DataFrame for visualization
df_pd = df_spark.toPandas()

# Filter out rows with NaN values in important columns
df_pd = df_pd.dropna(subset=['time_position', 'longitude', 'latitude'])

# Convert time_position to PST
df_pd['time_position_pst'] = df_pd['time_position'].apply(lambda x: datetime.fromtimestamp(x, timezone.utc).strftime('%Y-%m-%d %H:%M:%S'))

# Create a Plotly scatter mapbox plot
fig = px.scatter_mapbox(df_pd, lat="latitude", lon="longitude", hover_name="callsign", hover_data=["origin_country", "velocity", "baro_altitude", "time_position_pst"],
                        color_discrete_sequence=["fuchsia"], zoom=3, height=600)

# Add lines connecting the contact points of the flight
for i in range(len(df_pd) - 1):
    fig.add_trace(go.Scattermapbox(
        mode="lines",
        lon=[df_pd.iloc[i]["longitude"], df_pd.iloc[i + 1]["longitude"]],
        lat=[df_pd.iloc[i]["latitude"], df_pd.iloc[i + 1]["latitude"]],
        line=dict(width=2, color='blue'),
        hoverinfo='text',
        hovertext=f"Timestamp: {df_pd.iloc[i]['time_position_pst']}"
    ))

fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

# Show the plot
fig.show()


# COMMAND ----------

