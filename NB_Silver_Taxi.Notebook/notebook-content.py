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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE silver.silver_taxi AS
# MAGIC SELECT
# MAGIC PULocationID AS pickup_location_id,
# MAGIC DOLocationID AS dropoff_location_id,
# MAGIC tpep_pickup_datetime AS pickup_datetime,
# MAGIC to_date(tpep_pickup_datetime) AS pickup_date,
# MAGIC passenger_count,
# MAGIC fare_amount,
# MAGIC trip_distance AS trip_distance_miles,  
# MAGIC total_amount AS total_amount_usd
# MAGIC FROM parquet.`Files/bronze_taxi/*.parquet`
# MAGIC WHERE trip_distance > 0 AND total_amount > 0;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
