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

openaq_api_key = ""
start_date = "2024-01-01"
end_date = ""
key_vault_url = ""
secret_name = "openaq-api-key"
page_limit = "1000"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import calendar
import json
import math
import time
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

import notebookutils

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

POLLUTANTS = {
    "no2": "NO₂", "no": "NO", "nox": "NOx", "so2": "SO₂", "co": "CO",
    "pm25": "PM2.5", "pm10": "PM10", "pm1": "PM1", "o3": "O₃",
    "pm03": "PM0.3 count", "um003": "PM0.3 count",
    "temperature": "Temperature (C)", "relativehumidity": "RH",
    "rh": "RH",
}
BASE_URL = "https://api.openaq.org"
REQUEST_DELAY_SECONDS = 1.1


page_limit = int(page_limit)
if not 1 <= page_limit <= 1000:
    raise ValueError("page_limit must be between 1 and 1000.")

today = datetime.now(timezone.utc).date()
month_number = today.year * 12 + today.month - 3
latest_month = date(month_number // 12, month_number % 12 + 1, 1)
latest_date = latest_month.replace(day=calendar.monthrange(latest_month.year, latest_month.month)[1])
first_date = date.fromisoformat(start_date)
last_date = date.fromisoformat(end_date) if end_date else latest_date
if first_date < date(2015, 1, 1) or first_date > last_date or last_date > latest_date:
    raise ValueError("Use a date range from 2015-01-01 through the last day of the month two months ago.")

if not openaq_api_key and key_vault_url:
    openaq_api_key = notebookutils.credentials.getSecret(key_vault_url, secret_name)
if not openaq_api_key:
    raise ValueError("Provide an OpenAQ API key or a Key Vault URL.")

nyc_timezone = ZoneInfo("America/New_York")
query_from = datetime.combine(first_date, datetime_time.min, nyc_timezone).astimezone(timezone.utc).isoformat()
query_to = datetime.combine(last_date + timedelta(days=1), datetime_time.min, nyc_timezone).astimezone(timezone.utc).isoformat()


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
            if item.get("isMobile"):
                continue
            coordinates = item.get("coordinates") or {}
            if coordinates.get("latitude") is None or coordinates.get("longitude") is None:
                continue
            sensors = item.get("sensors") or []
            for sensor in sensors:
                parameter = sensor.get("parameter") or {}
                pollutant_name = POLLUTANTS.get(parameter.get("name", "").lower())
                if pollutant_name is None:
                    continue

                rows.append(
                    {
                        "LocationID": int(item["id"]),
                        "locality": item.get("locality"),
                        "latitude": float(coordinates["latitude"]),
                        "longitude": float(coordinates["longitude"]),
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
    page = 1

    while True:
        payload = _request_json(
            f"v3/sensors/{sensor_id}/days",
            {
                "limit": str(page_limit),
                "page": str(page),
                "date_from": query_from,
                "date_to": query_to,
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
            local_from = _parse_utc((period.get("datetimeFrom") or {}).get("local"))
            if local_from is None or not first_date <= local_from.date() <= last_date:
                continue
            coverage = item.get("coverage") or {}
            flags = item.get("flagInfo") or {}
            dt_to_parsed = _parse_utc(dt_to)
            rows.append(
                {
                    "value": float(item["value"]) if item.get("value") is not None else None,
                    "coverage_percent": float(coverage["percentCoverage"]) if coverage.get("percentCoverage") is not None else None,
                    "has_flags": bool(flags.get("hasFlags", False)),
                    "utc_timestamp": dt_from_parsed,
                    "utc_end_timestamp": dt_to_parsed,
                    "measurement_date": local_from.date(),
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

if not any(
    row["value"] is not None and math.isfinite(row["value"])
    and row["utc_timestamp"] is not None and not row["has_flags"]
    and row["coverage_percent"] is not None and row["coverage_percent"] >= 75
    for row in measurement_rows
):
    raise RuntimeError("OpenAQ returned no usable measurements for the selected dates.")

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
        StructField("coverage_percent", DoubleType(), True),
        StructField("has_flags", BooleanType(), False),
        StructField("utc_timestamp", TimestampType(), True),
        StructField("utc_end_timestamp", TimestampType(), True),
        StructField("measurement_date", DateType(), True),
        StructField("measurement_hour", StringType(), True),
        StructField("hour_of_day", LongType(), True),
    ]
)

measurement_df = spark.createDataFrame(measurement_rows, schema=measurement_schema)
measurement_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.raw_openaq_full")

location_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("dbo.raw_openaq_locations")

print(
    f"Saved OpenAQ bronze tables: {len(location_rows)} location-sensor rows, "
    f"{len(measurement_rows)} daily history rows."
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
