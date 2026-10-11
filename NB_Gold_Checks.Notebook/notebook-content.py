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

from datetime import datetime, timezone

tables = {
    "gold.DimDate": ["DateID"],
    "gold.DimZone": ["ZoneID"],
    "gold.DimFX": ["DateID"],
    "gold.DimGDP": ["year"],
    "gold.FactTaxiDaily": ["DateID", "ZoneID"],
    "gold.FactAirQualityDaily": ["DateID", "ZoneID"],
}
summary = []
checked_at = datetime.now(timezone.utc)
for table, keys in tables.items():
    count = spark.table(table).count()
    if count == 0:
        raise ValueError(f"Empty table: {table}")
    columns = ", ".join(keys)
    if spark.sql(f"SELECT {columns} FROM {table} GROUP BY {columns} HAVING COUNT(*) > 1").limit(1).count():
        raise ValueError(f"Duplicate keys in {table}")
    null_keys = " OR ".join(f"{key} IS NULL" for key in keys)
    if spark.sql(f"SELECT 1 FROM {table} WHERE {null_keys}").limit(1).count():
        raise ValueError(f"Missing keys in {table}")
    summary.append((table, count, checked_at))

for fact in ("gold.FactTaxiDaily", "gold.FactAirQualityDaily"):
    for dimension, key in (("gold.DimDate", "DateID"), ("gold.DimZone", "ZoneID")):
        query = f"SELECT f.{key} FROM {fact} f LEFT ANTI JOIN {dimension} d ON f.{key} = d.{key}"
        if spark.sql(query).limit(1).count():
            raise ValueError(f"Missing {key} in {dimension} for {fact}")

if spark.sql("SELECT 1 FROM gold.FactTaxiDaily WHERE EurPerUsdRate IS NULL OR EurPerUsdRate <= 0").limit(1).count():
    raise ValueError("Taxi revenue is missing an exchange rate.")
if spark.sql("SELECT 1 FROM gold.DimFX WHERE SourceRateDate > DateID").limit(1).count():
    raise ValueError("A future exchange rate was used.")

silver_trips = spark.table("silver.silver_taxi").count()
gold_trips = spark.sql("SELECT SUM(TotalTrips) FROM gold.FactTaxiDaily").first()[0]
if silver_trips != gold_trips:
    raise ValueError("Taxi trip counts differ between Silver and Gold.")
revenue_mismatch = spark.sql("""
    WITH expected AS (
        SELECT pickup_date, pickup_location_id, SUM(total_amount_usd) AS total_amount,
            SUM(fare_amount) AS total_fare
        FROM silver.silver_taxi
        GROUP BY pickup_date, pickup_location_id
    )
    SELECT 1 FROM expected e
    LEFT JOIN gold.FactTaxiDaily t ON e.pickup_date = t.DateID AND e.pickup_location_id = t.ZoneID
    WHERE t.TotalAmount IS NULL OR t.TotalAmount <> e.total_amount
        OR t.TotalFareAmount IS NULL OR t.TotalFareAmount <> e.total_fare
""")
if revenue_mismatch.limit(1).count():
    raise ValueError("Taxi revenue differs between Silver and Gold.")
if not spark.sql("SELECT 1 FROM gold.FactTaxiDaily t INNER JOIN gold.FactAirQualityDaily aq ON t.DateID = aq.DateID AND t.ZoneID = aq.ZoneID").limit(1).count():
    raise ValueError("Taxi and air quality data have no matching date and zone.")

spark.createDataFrame(summary, "TableName STRING, RowCount LONG, CheckedAt TIMESTAMP").write.format(
    "delta"
).mode("overwrite").saveAsTable("gold.DataQualitySummary")
for table, count, _ in summary:
    print(f"{table}: {count} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
