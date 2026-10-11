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

# Default values are overridden by the pipeline activity parameters.
openaq_api_key = ""
history_days = "730"
page_limit = "1000"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SUPPORTED_POLLUTANTS = {
    "NO₂",
    "NO",
    "NOx",
    "SO₂",
    "CO",
    "PM2.5",
    "PM1",
    "PM0.3 count",
    "Temperature (C)",
    "RH",
}
BASE_URL = "https://api.openaq.org"
REQUEST_DELAY_SECONDS = 1.1


def _read_argument(name: str, default: str) -> str:
    try:
        value = mssparkutils.notebook.getArgument(name, default)
        if value not in (None, ""):
            return value
    except Exception:
        pass

    try:
        value = mssparkutils.notebook.params.get(name)
        if value not in (None, ""):
            return value
    except Exception:
        pass

    return default


openaq_api_key = _read_argument("openaq_api_key", openaq_api_key)
history_days = int(_read_argument("history_days", history_days))
page_limit = int(_read_argument("page_limit", page_limit))

if not openaq_api_key:
    raise ValueError("OpenAQ API key was not provided to NB_Bronze_OpenAQ.")


def _request_json(relative_path: str, query: dict[str, str | int]) -> dict:
    url = f"{BASE_URL}/{relative_path}?{urlencode(query)}"
    headers = {
        "Accept": "application/json",
        "X-API-Key": openaq_api_key,
        "User-Agent": "fabric-learning-project/1.0",
    }

    last_error = None
    for attempt in range(6):
        try:
            request = Request(url, headers=headers, method="GET")
            with urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode("utf-8"))
                time.sleep(REQUEST_DELAY_SECONDS)
                return payload
        except HTTPError as exc:
            last_error = exc
            body = exc.read().decode("utf-8", errors="ignore")
            if exc.code == 429:
                retry_after = exc.headers.get("Retry-After")
                sleep_seconds = float(retry_after) if retry_after else max(5, 2**attempt)
                print(f"OpenAQ rate limited. Sleeping for {sleep_seconds} seconds.")
                time.sleep(sleep_seconds)
                continue
            if exc.code in {500, 502, 503, 504}:
                time.sleep(max(5, 2**attempt))
                continue
            raise RuntimeError(f"OpenAQ request failed ({exc.code}) for {url}: {body}") from exc
        except URLError as exc:
            last_error = exc
            time.sleep(max(5, 2**attempt))

    raise RuntimeError(f"OpenAQ request failed after retries for {url}") from last_error


def _parse_utc(value: str | None):
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _fetch_locations() -> list[dict]:
    rows = []
    page = 1
    while True:
        payload = _request_json(
            "v3/locations",
            {
                "coordinates": "40.7128,-74.0060",
                "radius": "25000",
                "limit": str(page_limit),
                "page": str(page),
            },
        )
        results = payload.get("results", [])
        if not results:
            break

        for item in results:
            coordinates = item.get("coordinates") or {}
            sensors = item.get("sensors") or []
            for sensor in sensors:
                parameter = sensor.get("parameter") or {}
                pollutant_name = parameter.get("displayName")
                if pollutant_name not in SUPPORTED_POLLUTANTS:
                    continue

                rows.append(
                    {
                        "LocationID": int(item["id"]),
                        "locality": item.get("locality"),
                        "latitude": coordinates.get("latitude"),
                        "longitude": coordinates.get("longitude"),
                        "SensorID": int(sensor["id"]),
                        "units": parameter.get("units"),
                        "pollutant_name": pollutant_name,
                    }
                )

        print(f"Fetched OpenAQ locations page {page} with {len(results)} locations.")
        page += 1

    unique_rows = {}
    for row in rows:
        unique_rows[(row["LocationID"], row["SensorID"])] = row
    return list(unique_rows.values())


def _fetch_sensor_days(sensor_id: int) -> list[dict]:
    rows = []
    date_from = (datetime.now(timezone.utc) - timedelta(days=history_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    page = 1

    while True:
        payload = _request_json(
            f"v3/sensors/{sensor_id}/days",
            {
                "limit": str(page_limit),
                "page": str(page),
                "date_from": date_from,
            },
        )
        results = payload.get("results", [])
        if not results:
            break

        for item in results:
            period = item.get("period") or {}
            dt_from = (period.get("datetimeFrom") or {}).get("utc")
            dt_to = (period.get("datetimeTo") or {}).get("utc")
            dt_from_parsed = _parse_utc(dt_from)
            dt_to_parsed = _parse_utc(dt_to)
            rows.append(
                {
                    "value": item.get("value"),
                    "utc_timestamp": dt_from_parsed,
                    "utc_end_timestamp": dt_to_parsed,
                    "measurement_date": dt_from_parsed.date() if dt_from_parsed else None,
                    "measurement_hour": dt_from_parsed.strftime("%H:%M:%S") if dt_from_parsed else None,
                    "hour_of_day": dt_from_parsed.hour if dt_from_parsed else None,
                }
            )

        if len(results) < page_limit:
            break
        page += 1

    return rows


spark.sql("CREATE SCHEMA IF NOT EXISTS dbo")

location_rows = _fetch_locations()
if not location_rows:
    raise RuntimeError("OpenAQ location query returned no matching NYC sensors.")

location_schema = StructType(
    [
        StructField("LocationID", LongType(), False),
        StructField("locality", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("SensorID", LongType(), False),
        StructField("units", StringType(), True),
        StructField("pollutant_name", StringType(), True),
    ]
)

location_df = spark.createDataFrame(location_rows, schema=location_schema)
spark.sql("DROP TABLE IF EXISTS dbo.raw_openaq_locations")
location_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.raw_openaq_locations")

measurement_rows = []
for index, row in enumerate(location_rows, start=1):
    sensor_days = _fetch_sensor_days(row["SensorID"])
    for sensor_day in sensor_days:
        measurement_rows.append(
            {
                **row,
                **sensor_day,
            }
        )
    if index % 10 == 0 or index == len(location_rows):
        print(f"Fetched OpenAQ daily history for {index}/{len(location_rows)} sensors.")

measurement_schema = StructType(
    [
        StructField("LocationID", LongType(), False),
        StructField("locality", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("SensorID", LongType(), False),
        StructField("units", StringType(), True),
        StructField("pollutant_name", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("utc_timestamp", TimestampType(), True),
        StructField("utc_end_timestamp", TimestampType(), True),
        StructField("measurement_date", DateType(), True),
        StructField("measurement_hour", StringType(), True),
        StructField("hour_of_day", LongType(), True),
    ]
)

measurement_df = spark.createDataFrame(measurement_rows, schema=measurement_schema)
spark.sql("DROP TABLE IF EXISTS dbo.raw_openaq_full")
measurement_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.raw_openaq_full")

print(
    f"Saved OpenAQ bronze tables: {len(location_rows)} location-sensor rows, "
    f"{len(measurement_rows)} daily history rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
