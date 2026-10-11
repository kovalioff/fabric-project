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
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC 
# MAGIC CREATE OR REPLACE TABLE gold.FactAirQualityDaily AS 
# MAGIC SELECT 
# MAGIC aq.measurement_date AS DateID, 
# MAGIC map.TaxiZoneID AS ZoneID, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'NO₂' THEN aq.value END), 2) AS NO2, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'NO' THEN aq.value END), 2) AS NO, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'NOx' THEN aq.value END), 2) AS NOx, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'SO₂' THEN aq.value END), 2) AS SO2, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'CO' THEN aq.value END), 2) AS CO, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'PM2.5' THEN aq.value END), 2) AS PM25, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'PM1' THEN aq.value END), 2) AS PM1, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'PM0.3 count' THEN aq.value END), 2) AS PM03_Count, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'Temperature (C)' THEN aq.value END), 2) AS TemperatureC, 
# MAGIC ROUND(AVG(CASE WHEN aq.pollutant_name = 'RH' THEN aq.value END), 2) AS HumidityRH 
# MAGIC FROM silver.silver_openaq_clean aq 
# MAGIC INNER JOIN silver.silver_openaq_zone_mapping map 
# MAGIC ON aq.LocationID = map.LocationID 
# MAGIC GROUP BY aq.measurement_date, map.TaxiZoneID;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
