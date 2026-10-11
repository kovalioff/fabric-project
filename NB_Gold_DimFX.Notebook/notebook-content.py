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
# MAGIC CREATE OR REPLACE TABLE gold.DimFX AS
# MAGIC SELECT 
# MAGIC date_key AS DateID,
# MAGIC usd_to_eur,
# MAGIC CASE
# MAGIC   WHEN usd_to_eur IS NOT NULL AND usd_to_eur <> 0 THEN ROUND(1 / usd_to_eur, 6)
# MAGIC   ELSE NULL
# MAGIC END AS eur_per_usd
# MAGIC FROM silver.silver_ecb_fx

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
