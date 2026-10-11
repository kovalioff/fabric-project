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
# MAGIC WITH daily AS (
# MAGIC     SELECT pickup_date AS DateID, pickup_location_id AS ZoneID, COUNT(*) AS TotalTrips,
# MAGIC         AVG(trip_distance_miles) AS AvgTripDistance,
# MAGIC         AVG(fare_amount) AS AvgFarePerTrip, SUM(fare_amount) AS TotalFareAmount,
# MAGIC         AVG(total_amount_usd) AS AvgRevenuePerTripUSD, SUM(total_amount_usd) AS TotalAmount
# MAGIC     FROM silver.silver_taxi
# MAGIC     GROUP BY pickup_date, pickup_location_id
# MAGIC )
# MAGIC SELECT t.DateID, t.ZoneID, t.TotalTrips, ROUND(t.AvgTripDistance, 2) AS AvgTripDistance,
# MAGIC     ROUND(t.AvgFarePerTrip, 2) AS AvgFarePerTrip, t.TotalFareAmount,
# MAGIC     ROUND(t.AvgRevenuePerTripUSD, 2) AS AvgRevenuePerTripUSD, t.TotalAmount,
# MAGIC     fx.eur_per_usd AS EurPerUsdRate,
# MAGIC     CAST(ROUND(t.AvgRevenuePerTripUSD * fx.eur_per_usd, 2) AS DECIMAL(18,2)) AS AvgRevenuePerTripEUR,
# MAGIC     CAST(ROUND(t.TotalAmount * fx.eur_per_usd, 2) AS DECIMAL(20,2)) AS TotalAmountEUR
# MAGIC FROM daily t
# MAGIC LEFT JOIN gold.DimFX fx ON t.DateID = fx.DateID;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
