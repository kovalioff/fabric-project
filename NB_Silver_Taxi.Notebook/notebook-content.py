# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "3163ca5c-40aa-452d-bd53-305c73cfe98f",
# META       "default_lakehouse_name": "lakehouse",
# META       "default_lakehouse_workspace_id": "1023cf7f-ee85-4dc6-9f17-84f7d7ad4175",
# META       "known_lakehouses": [
# META         {
# META           "id": "3163ca5c-40aa-452d-bd53-305c73cfe98f"
# META         }
# META       ]
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

start_date = "2024-01-01"
end_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import calendar
from datetime import date, datetime, timezone

from pyspark.sql import functions as F


today = datetime.now(timezone.utc).date()
month_number = today.year * 12 + today.month - 3
latest_month = date(month_number // 12, month_number % 12 + 1, 1)
latest_date = latest_month.replace(day=calendar.monthrange(latest_month.year, latest_month.month)[1])
first_date = date.fromisoformat(start_date)
last_date = date.fromisoformat(end_date) if end_date else latest_date
if first_date < date(2015, 1, 1) or first_date > last_date or last_date > latest_date:
    raise ValueError("Use a date range from 2015-01-01 through the last day of the month two months ago.")

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
source = None
month = first_date.replace(day=1)
while month <= last_date:
    monthly = spark.read.parquet(f"Files/bronze_taxi/yellow_tripdata_{month:%Y-%m}.parquet").select(
        F.col("PULocationID").cast("int"),
        F.col("DOLocationID").cast("int"),
        F.col("tpep_pickup_datetime").cast("timestamp"),
        F.col("tpep_dropoff_datetime").cast("timestamp"),
        F.col("passenger_count").cast("int"),
        F.col("fare_amount").cast("decimal(18,2)"),
        F.col("trip_distance").cast("double"),
        F.col("total_amount").cast("decimal(18,2)"),
        F.lit(f"{month:%Y-%m}").alias("source_month"),
    )
    source = monthly if source is None else source.unionByName(monthly)
    month = date(month.year + month.month // 12, month.month % 12 + 1, 1)
clean = (
    source.filter(F.date_format("tpep_pickup_datetime", "yyyy-MM") == F.col("source_month"))
    .filter(F.to_date("tpep_pickup_datetime").between(first_date, last_date))
    .filter(F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
    .filter(F.col("trip_distance").between(0.01, 500))
    .filter(F.col("fare_amount") >= 0)
    .filter(F.col("total_amount") > 0)
    .filter(~F.isnan("fare_amount") & ~F.isnan("total_amount"))
    .filter(F.col("PULocationID").between(1, 265) & F.col("DOLocationID").between(1, 265))
    .select(
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("tpep_dropoff_datetime").alias("dropoff_datetime"),
        F.to_date("tpep_pickup_datetime").alias("pickup_date"),
        F.col("passenger_count").cast("int"),
        F.col("fare_amount").cast("decimal(18,2)"),
        F.col("trip_distance").cast("double").alias("trip_distance_miles"),
        F.col("total_amount").cast("decimal(18,2)").alias("total_amount_usd"),
    )
    .filter(F.col("fare_amount").isNotNull() & F.col("total_amount_usd").isNotNull())
)
if clean.limit(1).count() == 0:
    raise ValueError("No valid taxi trips in the selected date range.")
clean.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.silver_taxi")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
