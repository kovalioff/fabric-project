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
# MAGIC CREATE OR REPLACE TABLE gold.FactTaxiDaily AS
# MAGIC WITH daily_fx AS (
# MAGIC   SELECT
# MAGIC     date_key AS DateID,
# MAGIC     CASE
# MAGIC       WHEN usd_to_eur IS NOT NULL AND usd_to_eur <> 0 THEN 1 / usd_to_eur
# MAGIC       ELSE NULL
# MAGIC     END AS eur_per_usd
# MAGIC   FROM silver.silver_ecb_fx
# MAGIC )
# MAGIC SELECT 
# MAGIC   t.pickup_date AS DateID,
# MAGIC   t.pickup_location_id AS ZoneID,
# MAGIC   COUNT(*) AS TotalTrips,
# MAGIC   ROUND(AVG(t.trip_distance_miles), 2) AS AvgTripDistance,
# MAGIC   ROUND(AVG(t.fare_amount), 2) AS AvgFarePerTrip,
# MAGIC   ROUND(SUM(t.fare_amount), 2) AS TotalFareAmount,
# MAGIC   ROUND(AVG(t.total_amount_usd), 2) AS AvgRevenuePerTripUSD,
# MAGIC   ROUND(SUM(t.total_amount_usd), 2) AS TotalAmount,
# MAGIC   ROUND(MAX(fx.eur_per_usd), 6) AS EurPerUsdRate,
# MAGIC   ROUND(AVG(t.total_amount_usd) * MAX(fx.eur_per_usd), 2) AS AvgRevenuePerTripEUR,
# MAGIC   ROUND(SUM(t.total_amount_usd) * MAX(fx.eur_per_usd), 2) AS TotalAmountEUR
# MAGIC FROM silver.silver_taxi t
# MAGIC LEFT JOIN daily_fx fx
# MAGIC   ON t.pickup_date = fx.DateID
# MAGIC WHERE t.pickup_location_id IS NOT NULL 
# MAGIC GROUP BY t.pickup_date, t.pickup_location_id;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
