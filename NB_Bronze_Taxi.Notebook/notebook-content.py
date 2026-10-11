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

# PARAMETERS CELL ********************

start_date = "2024-01-01"
end_date = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import calendar
import shutil
from datetime import date, datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.request import Request, urlopen

import notebookutils
import pyarrow.parquet as pq


today = datetime.now(timezone.utc).date()
month_number = today.year * 12 + today.month - 3
latest_month = date(month_number // 12, month_number % 12 + 1, 1)
latest_date = latest_month.replace(day=calendar.monthrange(latest_month.year, latest_month.month)[1])
first_date = date.fromisoformat(start_date)
last_date = date.fromisoformat(end_date) if end_date else latest_date
if first_date < date(2015, 1, 1) or first_date > last_date or last_date > latest_date:
    raise ValueError("Use a date range from 2015-01-01 through the last day of the month two months ago.")

folder = "Files/bronze_taxi"
notebookutils.fs.mkdirs(folder)
month = first_date.replace(day=1)
while month <= last_date:
    filename = f"yellow_tripdata_{month:%Y-%m}.parquet"
    target = f"{folder}/{filename}"
    if notebookutils.fs.exists(target):
        if spark.read.parquet(target).limit(1).count() == 0:
            raise ValueError(f"Empty taxi file: {filename}")
        print(f"Already loaded: {filename}")
    else:
        url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{filename}"
        with TemporaryDirectory() as directory:
            path = Path(directory) / filename
            request = Request(url, headers={"User-Agent": "fabric-project/1.0"})
            with urlopen(request, timeout=180) as response, path.open("wb") as output:
                shutil.copyfileobj(response, output)
            metadata = pq.read_metadata(path)
            if metadata.num_rows == 0:
                raise ValueError(f"Empty taxi file: {filename}")
            staging = f"Files/taxi_staging/{filename}"
            notebookutils.fs.mkdirs("Files/taxi_staging")
            if not notebookutils.fs.cp(path.as_uri(), staging):
                raise RuntimeError(f"Could not upload {filename}")
            if not notebookutils.fs.mv(staging, target, create_path=True, overwrite=True):
                raise RuntimeError(f"Could not publish {filename}")
            print(f"Loaded {filename}: {metadata.num_rows} trips")
    month = date(month.year + month.month // 12, month.month % 12 + 1, 1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
