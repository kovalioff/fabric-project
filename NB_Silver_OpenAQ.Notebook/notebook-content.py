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
# MAGIC     SELECT *, LOWER(REPLACE(REPLACE(REPLACE(REPLACE(units, 'µ', 'u'), 'μ', 'u'), '³', '3'), ' ', '')) AS unit_key,
# MAGIC         ROW_NUMBER() OVER (
# MAGIC             PARTITION BY LocationID, SensorID, utc_timestamp ORDER BY utc_end_timestamp DESC
# MAGIC         ) AS rn
# MAGIC     FROM dbo.raw_openaq_full
# MAGIC     WHERE value IS NOT NULL AND NOT ISNAN(value)
# MAGIC         AND utc_timestamp IS NOT NULL AND measurement_date IS NOT NULL
# MAGIC         AND COALESCE(has_flags, FALSE) = FALSE AND coverage_percent >= 75
# MAGIC ), normalized AS (
# MAGIC     SELECT *, CASE
# MAGIC         WHEN pollutant_name IN ('NO₂', 'NO', 'NOx', 'SO₂', 'CO', 'O₃', 'PM2.5', 'PM10', 'PM1')
# MAGIC             AND unit_key IN ('ug/m3', 'ug/m^3') THEN value
# MAGIC         WHEN pollutant_name IN ('NO₂', 'NO', 'NOx', 'SO₂', 'CO', 'O₃', 'PM2.5', 'PM10', 'PM1')
# MAGIC             AND unit_key IN ('mg/m3', 'mg/m^3') THEN value * 1000
# MAGIC         WHEN pollutant_name IN ('NO₂', 'NO', 'SO₂', 'CO', 'O₃') AND unit_key IN ('ppm', 'ppb') THEN
# MAGIC             value * CASE pollutant_name
# MAGIC                 WHEN 'NO₂' THEN 46.0055 WHEN 'NO' THEN 30.0061 WHEN 'SO₂' THEN 64.066
# MAGIC                 WHEN 'CO' THEN 28.0101 WHEN 'O₃' THEN 47.9982
# MAGIC             END / 24.45 * CASE WHEN unit_key = 'ppm' THEN 1000 ELSE 1 END
# MAGIC         WHEN pollutant_name = 'Temperature (C)' AND unit_key IN ('c', '°c', 'degc', 'celsius') THEN value
# MAGIC         WHEN pollutant_name = 'Temperature (C)' AND unit_key IN ('f', '°f', 'degf', 'fahrenheit') THEN (value - 32) * 5 / 9
# MAGIC         WHEN pollutant_name = 'Temperature (C)' AND unit_key IN ('k', 'kelvin') THEN value - 273.15
# MAGIC         WHEN pollutant_name = 'RH' AND unit_key IN ('%', 'percent') THEN value
# MAGIC         WHEN pollutant_name = 'PM0.3 count' AND unit_key IN ('particles/cm3', 'particles/cm^3') THEN value
# MAGIC     END AS normalized_value
# MAGIC     FROM source_rows WHERE rn = 1
# MAGIC )
# MAGIC SELECT LocationID, SensorID, normalized_value AS value, utc_timestamp, utc_end_timestamp,
# MAGIC     measurement_date, hour_of_day, locality, latitude, longitude, pollutant_name,
# MAGIC     CASE WHEN pollutant_name = 'Temperature (C)' THEN '°C'
# MAGIC         WHEN pollutant_name = 'RH' THEN '%'
# MAGIC         WHEN pollutant_name = 'PM0.3 count' THEN 'particles/cm3'
# MAGIC         ELSE 'ug/m3' END AS units,
# MAGIC     value AS source_value, units AS source_units, coverage_percent
# MAGIC FROM normalized
# MAGIC WHERE normalized_value IS NOT NULL AND NOT ISNAN(normalized_value)
# MAGIC     AND (pollutant_name = 'Temperature (C)' OR normalized_value >= 0)
# MAGIC     AND (pollutant_name <> 'RH' OR normalized_value <= 100);

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
