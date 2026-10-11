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
# MAGIC CREATE OR REPLACE TABLE gold.DimDate AS
# MAGIC SELECT 
# MAGIC CAST(date_val AS DATE) AS DateID,
# MAGIC YEAR(date_val) AS Year,
# MAGIC MONTH(date_val) AS Month,
# MAGIC DAY(date_val) AS Day,
# MAGIC DATE_FORMAT(date_val, 'EEEE') AS DayName,
# MAGIC DATE_FORMAT(date_val, 'MMMM') AS MonthName
# MAGIC FROM (
# MAGIC SELECT EXPLODE(SEQUENCE(TO_DATE('2015-01-01'), TO_DATE('2099-12-31'), INTERVAL 1 DAY)) AS date_val
# MAGIC );

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
