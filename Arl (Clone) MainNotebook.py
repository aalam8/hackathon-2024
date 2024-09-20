# Databricks notebook source


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

df = spark.read.format(file_type).option("header", True).schema(schema).load(file_location)
save_location = file_location + "saved/"
df.writeTo("flightDelta").createOrReplace()
# df.write.format("delta").mode("overwrite").save(save_location)
dbutils.notebook.exit(save_location) 

# COMMAND ----------

# returned_table = dbutils.notebook.run("FlightPrices", 60)
# display(spark.read.format("delta").load(returned_table))

people_df = spark.read.table("flightDelta")
display(people_df)

# COMMAND ----------

# DBTITLE 1,Create Spoof Flight Data
from pyspark.sql import Row

data = [
    Row(Airline='CheeseAir', Date_Of_Journey='2023-09-01', Source='Delhi', Destination='Mumbai', Route='DEL -> BOM', Dep_Time='10:00', Arrival_Time='12:00', Duration='2h', Total_Stops='non-stop', Additional_Info='No info', Price=1),
    Row(Airline='CheeseAir', Date_Of_Journey='2023-09-02', Source='Mumbai', Destination='Delhi', Route='BOM -> DEL', Dep_Time='14:00', Arrival_Time='16:30', Duration='2h 30m', Total_Stops='non-stop', Additional_Info='No info', Price=21),
    Row(Airline='CheeseAir', Date_Of_Journey='2023-09-03', Source='Bangalore', Destination='Kolkata', Route='BLR -> CCU', Dep_Time='06:00', Arrival_Time='08:30', Duration='2h 30m', Total_Stops='non-stop', Additional_Info='No info', Price=42),
    Row(Airline='CheeseAir', Date_Of_Journey='2023-09-04', Source='Chennai', Destination='Hyderabad', Route='MAA -> HYD', Dep_Time='09:00', Arrival_Time='10:30', Duration='1h 30m', Total_Stops='non-stop', Additional_Info='No info', Price=1337),
    Row(Airline='CheeseAir', Date_Of_Journey='2023-09-05', Source='Pune', Destination='Delhi', Route='PNQ -> DEL', Dep_Time='18:00', Arrival_Time='20:30', Duration='2h 30m', Total_Stops='non-stop', Additional_Info='No info', Price=17)
]

df_flight_updates = spark.createDataFrame(data, schema)
display(df_flight_updates)

# COMMAND ----------

# DBTITLE 1,Update Delta Table with Spoof Data (Cell Above)
from delta.tables import DeltaTable
deltaTable = DeltaTable.forName(spark, 'flightDelta')
# this is to merge if everything matches except the price
(deltaTable
  .merge(
    df_flight_updates.alias("flightDelta_updates"),
    "flightDelta.Airline = flightDelta_updates.Airline AND \
     flightDelta.Date_Of_Journey = flightDelta_updates.Date_Of_Journey AND \
     flightDelta.Source = flightDelta_updates.Source AND \
     flightDelta.Destination = flightDelta_updates.Destination AND \
     flightDelta.Route = flightDelta_updates.Route AND \
     flightDelta.Dep_Time = flightDelta_updates.Dep_Time AND \
     flightDelta.Arrival_Time = flightDelta_updates.Arrival_Time AND \
     flightDelta.Duration = flightDelta_updates.Duration AND \
     flightDelta.Total_Stops = flightDelta_updates.Total_Stops AND \
     flightDelta.Additional_Info = flightDelta_updates.Additional_Info")
  .whenMatchedUpdateAll()
  .whenNotMatchedInsertAll()
  .execute()
)

df_NewFlightDelta = spark.read.table("flightDelta")
df_filtered_CheeseAir = df_NewFlightDelta.filter(df_NewFlightDelta["Airline"] == "CheeseAir")
display(df_filtered_CheeseAir)

# COMMAND ----------

deltaTable = DeltaTable.forName(spark, 'flightDelta')
display(deltaTable.history())
deltaHistory = deltaTable.history()
display(deltaHistory.where("version == 0"))
# Or:
display(deltaHistory.where("timestamp == '2024-05-15T22:43:15.000+00:00'"))

# COMMAND ----------

# Update a specific row in the Delta table
deltaTable = DeltaTable.forName(spark, 'flightDelta')
deltaTable.update(
    condition="Airline = 'CheeseAir' AND Date_Of_Journey = '2023-09-01'",
    set={"Price": "5000"}
)

# Display the updated Delta table
df_updated_flightDelta = spark.read.table("flightDelta")
display(df_updated_flightDelta.filter(df_updated_flightDelta["Airline"] == "CheeseAir"))

# Display the history of the Delta table
deltaHistory = deltaTable.history()
display(deltaHistory)

# COMMAND ----------

from pyspark.sql.functions import col
import ipywidgets as widgets
from IPython.display import display as ipy_display, HTML

# Load the flight data
df_flights = spark.read.table("flightDelta")

# Create widgets for user input
# Get unique values for source and destination
unique_sources = df_flights.select("Source").distinct().rdd.map(lambda r: r[0]).collect()
unique_destinations = df_flights.select("Destination").distinct().rdd.map(lambda r: r[0]).collect()

# source_widget = widgets.Dropdown(
#     options=unique_sources,
#     value=unique_sources[0] if unique_sources else '',
#     description='Source:',
#     disabled=False
# )

destination_widget = widgets.Dropdown(
    options=unique_destinations,
    value=unique_destinations[0] if unique_destinations else '',
    description='Destination:',
    disabled=False
)

# Function to filter and display flights based on user input
def filter_flights(destination):
    filtered_df = df_flights
    if destination:
        filtered_df = filtered_df.filter(col("Destination") == destination)
    # Select only the airline, source, and price columns
        filtered_df = filtered_df.select("Airline", "Source", "Price")
    # Convert the filtered DataFrame to HTML and display it
        filtered_df = filtered_df.orderBy(col("Price"))
    html = filtered_df._jdf.showString(20, 20, False)
    ipy_display(HTML("<pre>{}</pre>".format(html)))

# Create an interactive UI
ui = widgets.VBox([destination_widget])
out = widgets.interactive_output(filter_flights, {'destination': destination_widget})

ipy_display(ui, out)