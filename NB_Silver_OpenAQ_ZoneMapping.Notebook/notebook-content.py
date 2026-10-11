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

%pip install geopandas==1.0.1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
from pyspark.sql.types import LongType, StructField, StructType

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")
with TemporaryDirectory() as directory:
    directory = Path(directory)
    for filename in ("taxi_zones.zip", "taxi_zone_lookup.csv"):
        url = f"https://d37ci6vzurychx.cloudfront.net/misc/{filename}"
        request = Request(url, headers={"User-Agent": "fabric-project/1.0"})
        with urlopen(request, timeout=60) as response:
            (directory / filename).write_bytes(response.read())
    with ZipFile(directory / "taxi_zones.zip") as archive:
        archive.extractall(directory)
    zones = gpd.read_file(next(directory.glob("**/*.shp"))).to_crs("EPSG:4326")
    reference = pd.read_csv(directory / "taxi_zone_lookup.csv", keep_default_na=False)

if reference.empty or reference["LocationID"].duplicated().any():
    raise ValueError("Taxi zone lookup must contain one row per LocationID.")
if not {264, 265}.issubset(set(reference["LocationID"])):
    raise ValueError("Taxi zone lookup is missing the special zones.")

stations = spark.read.table("dbo.raw_openaq_locations").select(
    "LocationID", "latitude", "longitude"
).distinct().toPandas()
if stations.empty or stations["LocationID"].duplicated().any():
    raise ValueError("OpenAQ station coordinates are missing or inconsistent.")

points = gpd.GeoDataFrame(
    stations,
    geometry=gpd.points_from_xy(stations["longitude"], stations["latitude"]),
    crs="EPSG:4326",
)
polygons = zones[["LocationID", "geometry"]].rename(columns={"LocationID": "TaxiZoneID"})
matched = gpd.sjoin(points, polygons, how="left", predicate="intersects")
mapping = matched[["LocationID", "TaxiZoneID"]].dropna().drop_duplicates()
if mapping.empty or mapping["LocationID"].duplicated().any():
    raise ValueError("Station mapping is empty or a station matches several zones.")

mapping_rows = [(int(row.LocationID), int(row.TaxiZoneID)) for row in mapping.itertuples()]
mapping_schema = StructType([
    StructField("LocationID", LongType(), False),
    StructField("TaxiZoneID", LongType(), False),
])
spark.createDataFrame(reference).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.silver_taxi_zone_reference")
spark.createDataFrame(mapping_rows, mapping_schema).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("silver.silver_openaq_zone_mapping")

print(f"Mapped {len(mapping)} of {len(stations)} OpenAQ stations to taxi zones.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
