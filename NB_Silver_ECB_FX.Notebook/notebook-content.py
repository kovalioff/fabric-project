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
# MAGIC CREATE OR REPLACE TABLE silver.silver_ecb_fx AS
# MAGIC WITH rates AS (
# MAGIC     SELECT CAST(TIME_PERIOD AS DATE) AS rate_date, MAX(CAST(OBS_VALUE AS DOUBLE)) AS usd_per_eur
# MAGIC     FROM dbo.ECB_FX
# MAGIC     WHERE TIME_PERIOD IS NOT NULL AND CAST(OBS_VALUE AS DOUBLE) > 0
# MAGIC     GROUP BY CAST(TIME_PERIOD AS DATE)
# MAGIC ), calendar AS (
# MAGIC     SELECT EXPLODE(SEQUENCE(MIN(rate_date), GREATEST(MAX(rate_date), CURRENT_DATE()), INTERVAL 1 DAY)) AS date_key
# MAGIC     FROM rates
# MAGIC ), filled AS (
# MAGIC     SELECT c.date_key,
# MAGIC         LAST(r.usd_per_eur, TRUE) OVER (
# MAGIC             ORDER BY c.date_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         ) AS usd_per_eur,
# MAGIC         LAST(r.rate_date, TRUE) OVER (
# MAGIC             ORDER BY c.date_key ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
# MAGIC         ) AS source_rate_date
# MAGIC     FROM calendar c
# MAGIC     LEFT JOIN rates r ON c.date_key = r.rate_date
# MAGIC )
# MAGIC SELECT date_key, usd_per_eur, 1 / usd_per_eur AS eur_per_usd, source_rate_date
# MAGIC FROM filled;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
