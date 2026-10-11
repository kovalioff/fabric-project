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

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

if not spark.catalog.tableExists("silver.silver_taxi_zone_reference"):
    raise ValueError(
        "silver.silver_taxi_zone_reference was not found. "
        "Run NB_Silver_OpenAQ_ZoneMapping before NB_Gold_DimZone."
    )

spark_dim_zone_df = (
    spark.read.table("silver.silver_taxi_zone_reference")
    .select("LocationID", "Borough", "Zone", "service_zone")
    .dropDuplicates(["LocationID"])
)
spark_dim_zone_df.write.format("delta").mode("overwrite").saveAsTable("gold.DimZone")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
