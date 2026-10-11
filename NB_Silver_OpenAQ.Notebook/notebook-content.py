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
# MAGIC CREATE OR REPLACE TABLE silver.silver_openaq_clean AS
# MAGIC WITH source_rows AS (
# MAGIC   SELECT
# MAGIC     LocationID,
# MAGIC     SensorID,
# MAGIC     CAST(value AS DOUBLE) AS value,
# MAGIC     CAST(utc_timestamp AS TIMESTAMP) AS utc_timestamp,
# MAGIC     CAST(utc_end_timestamp AS TIMESTAMP) AS utc_end_timestamp,
# MAGIC     CAST(measurement_date AS DATE) AS measurement_date,
# MAGIC     CAST(hour_of_day AS INT) AS hour_of_day,
# MAGIC     locality,
# MAGIC     latitude,
# MAGIC     longitude,
# MAGIC     pollutant_name,
# MAGIC     units,
# MAGIC     ROW_NUMBER() OVER (
# MAGIC       PARTITION BY LocationID, SensorID, utc_timestamp
# MAGIC       ORDER BY utc_end_timestamp DESC
# MAGIC     ) AS rn
# MAGIC   FROM dbo.raw_openaq_full
# MAGIC   WHERE value IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   LocationID,
# MAGIC   SensorID,
# MAGIC   value,
# MAGIC   utc_timestamp,
# MAGIC   utc_end_timestamp,
# MAGIC   measurement_date,
# MAGIC   hour_of_day,
# MAGIC   locality,
# MAGIC   latitude,
# MAGIC   longitude,
# MAGIC   pollutant_name,
# MAGIC   units
# MAGIC FROM source_rows
# MAGIC WHERE rn = 1;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
