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

%pip install geopandas

import urllib.request
import geopandas as gpd
from pathlib import Path
from shapely.geometry import Point
from zipfile import ZipFile

spark.sql("CREATE SCHEMA IF NOT EXISTS silver")

tmp_dir = Path("/tmp/taxi_zones")
zip_path = tmp_dir / "taxi_zones.zip"

tmp_dir.mkdir(parents=True, exist_ok=True)

opener = urllib.request.build_opener()
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)')]
urllib.request.install_opener(opener)
urllib.request.urlretrieve("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip", zip_path)

with ZipFile(zip_path, "r") as archive:
    archive.extractall(tmp_dir)

shp_files = list(tmp_dir.glob("**/*.shp"))
if not shp_files:
    raise ValueError("Shapefile not found in the downloaded archive")
shapefile_path = shp_files[0]

gdf_zones = gpd.read_file(shapefile_path).to_crs("EPSG:4326")
column_lookup = {column.lower(): column for column in gdf_zones.columns}
required_columns = ["locationid", "borough", "zone", "service_zone"]
missing_columns = [column for column in required_columns if column not in column_lookup]

if missing_columns:
    raise ValueError(
        "Taxi zone shapefile is missing expected columns: "
        + ", ".join(sorted(missing_columns))
    )

zone_reference = gdf_zones[
    [
        column_lookup["locationid"],
        column_lookup["borough"],
        column_lookup["zone"],
        column_lookup["service_zone"],
    ]
].rename(
    columns={
        column_lookup["locationid"]: "LocationID",
        column_lookup["borough"]: "Borough",
        column_lookup["zone"]: "Zone",
        column_lookup["service_zone"]: "service_zone",
    }
)
zone_reference["LocationID"] = zone_reference["LocationID"].astype(int)

spark.createDataFrame(zone_reference).write.format("delta").mode("overwrite").saveAsTable(
    "silver.silver_taxi_zone_reference"
)

zone_polygons = gdf_zones.rename(columns={column_lookup["locationid"]: "TaxiZoneID"})
openaq_df = spark.read.table("silver.silver_openaq_clean").select("LocationID", "latitude", "longitude").distinct().toPandas()

geometry = [Point(xy) for xy in zip(openaq_df['longitude'], openaq_df['latitude'])]
gdf_stations = gpd.GeoDataFrame(openaq_df, crs="EPSG:4326", geometry=geometry)

mapped_stations = gpd.sjoin(gdf_stations, zone_polygons, how="left", predicate="within")
final_mapping = mapped_stations[['LocationID', 'TaxiZoneID']].dropna(subset=['TaxiZoneID'])
final_mapping['TaxiZoneID'] = final_mapping['TaxiZoneID'].astype(int)

spark_mapping_df = spark.createDataFrame(final_mapping)
spark_mapping_df.write.format("delta").mode("overwrite").saveAsTable("silver.silver_openaq_zone_mapping")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
