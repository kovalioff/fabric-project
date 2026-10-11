import re
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


ROOT = Path(__file__).resolve().parents[1]


def query(name):
    source = (ROOT / f"{name}.Notebook/notebook-content.py").read_text(encoding="utf-8")
    sql = "\n".join(line.removeprefix("# MAGIC ") for line in source.splitlines() if line.startswith("# MAGIC "))
    result = re.search(r"CREATE OR REPLACE TABLE \S+ AS\s*(.*)", sql, re.DOTALL).group(1)
    return result.replace("dbo.", "").replace("silver.", "").replace("gold.", "").rstrip(";\n")


def test_fx_uses_previous_rate_on_weekend(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW ECB_FX AS
        SELECT * FROM VALUES ('2024-01-05', 1.10), ('2024-01-08', 1.20)
        AS rates(TIME_PERIOD, OBS_VALUE)""")
    rates = spark.sql(query("NB_Silver_ECB_FX"))
    rates.createOrReplaceTempView("silver_ecb_fx")
    weekend = rates.filter("date_key = DATE '2024-01-07'").first()
    monday = rates.filter("date_key = DATE '2024-01-08'").first()
    assert weekend.usd_per_eur == pytest.approx(1.10)
    assert weekend.eur_per_usd == pytest.approx(1 / 1.10)
    assert weekend.source_rate_date == date(2024, 1, 5)
    assert monday.source_rate_date == date(2024, 1, 8)
    spark.sql(query("NB_Gold_DimFX")).createOrReplaceTempView("DimFX")


def test_taxi_revenue_and_counts_survive_aggregation(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW silver_taxi AS
        SELECT DATE '2024-01-07' AS pickup_date, 1 AS pickup_location_id,
            CAST(fare AS DECIMAL(18,2)) AS fare_amount,
            CAST(total AS DECIMAL(18,2)) AS total_amount_usd, distance AS trip_distance_miles
        FROM VALUES (10.00, 11.00, 2.0), (20.00, 22.00, 4.0) AS trips(fare, total, distance)""")
    spark.sql("""CREATE OR REPLACE TEMP VIEW DimFX AS
        SELECT DATE '2024-01-07' AS DateID, 1 / 1.1 AS eur_per_usd""")
    result = spark.sql(query("NB_Gold_FactTaxi")).first()
    assert result.TotalTrips == 2
    assert result.TotalAmount == Decimal("33.00")
    assert result.TotalFareAmount == Decimal("30.00")
    assert result.TotalAmountEUR == Decimal("30.00")
    assert result.AvgRevenuePerTripEUR == Decimal("15.00")


def test_air_quality_normalizes_units_and_rejects_bad_measurements(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW raw_openaq_full AS
        SELECT 100 AS LocationID, sensor AS SensorID, value, pollutant_name, units,
            coverage AS coverage_percent, flags AS has_flags,
            TIMESTAMP '2024-01-07 05:00:00' AS utc_timestamp,
            TIMESTAMP '2024-01-08 05:00:00' AS utc_end_timestamp,
            DATE '2024-01-07' AS measurement_date, 5 AS hour_of_day,
            'NYC' AS locality, 40.7 AS latitude, -74.0 AS longitude
        FROM VALUES
            (1, 0.01, 'NO₂', 'ppm', 100.0, false),
            (2, 18.0, 'NO₂', 'µg/m³', 100.0, false),
            (3, 50.0, 'PM2.5', 'ug/m3', 50.0, false),
            (4, 90.0, 'PM2.5', 'ug/m3', 100.0, true),
            (5, 68.0, 'Temperature (C)', '°F', 100.0, false),
            (6, 150.0, 'RH', '%', 100.0, false),
            (7, 1.0, 'PM2.5', 'unknown', 100.0, false),
            (8, -1.0, 'PM2.5', 'ug/m3', 100.0, false)
        AS measurements(sensor, value, pollutant_name, units, coverage, flags)""")
    clean = spark.sql(query("NB_Silver_OpenAQ"))
    rows = {row.SensorID: row for row in clean.collect()}
    assert set(rows) == {1, 2, 5}
    assert float(rows[1].value) == pytest.approx(0.01 * 46.0055 / 24.45 * 1000)
    assert rows[1].units == "ug/m3"
    assert float(rows[5].value) == pytest.approx(20)
    clean.createOrReplaceTempView("silver_openaq_clean")
    spark.sql("""CREATE OR REPLACE TEMP VIEW silver_openaq_zone_mapping AS
        SELECT 100 AS LocationID, 1 AS TaxiZoneID""")
    fact = spark.sql(query("NB_Gold_FactAirQuality")).first()
    assert fact.ZoneID == 1
    assert fact.SensorCount == 3
    assert fact.StationCount == 1
    assert fact.PM25 is None
    assert fact.TemperatureC == pytest.approx(20)


def test_special_taxi_zones_exist_in_dimension(spark):
    spark.sql("""CREATE OR REPLACE TEMP VIEW silver_taxi_zone_reference AS
        SELECT * FROM VALUES (264, 'Unknown', 'N/A', 'N/A'), (265, 'N/A', 'Outside of NYC', 'N/A')
        AS zones(LocationID, Borough, Zone, service_zone)""")
    rows = spark.sql(query("NB_Gold_DimZone")).collect()
    assert {row.ZoneID for row in rows} == {264, 265}


def test_taxi_filters_source_month_and_invalid_trips(spark, tmp_path):
    filename = tmp_path / "yellow_tripdata_2024-01.parquet"
    pickups = [datetime(2024, 1, 7, 12), datetime(2023, 12, 31, 12), datetime(2024, 1, 8, 12), datetime(2024, 1, 9, 12)]
    data = pa.table({
        "PULocationID": [1, 1, 1, 264], "DOLocationID": [2, 2, 2, 265],
        "tpep_pickup_datetime": pickups,
        "tpep_dropoff_datetime": [value.replace(hour=13) for value in pickups],
        "passenger_count": [1, 1, 1, None], "fare_amount": [10.0, 10.0, 10.0, 20.0],
        "trip_distance": [2.0, 2.0, -1.0, 3.0], "total_amount": [11.0, 11.0, 11.0, 22.0],
    })
    pq.write_table(data, filename)
    pq.write_table(pa.table({
        "PULocationID": pa.array([1], type=pa.int32()),
        "DOLocationID": pa.array([2], type=pa.int32()),
        "tpep_pickup_datetime": [datetime(2024, 2, 7, 12)],
        "tpep_dropoff_datetime": [datetime(2024, 2, 7, 13)],
        "passenger_count": pa.array([1.0], type=pa.float64()),
        "fare_amount": [10.0], "trip_distance": [2.0], "total_amount": [11.0],
    }), tmp_path / "yellow_tripdata_2024-02.parquet")
    source = (ROOT / "NB_Silver_Taxi.Notebook/notebook-content.py").read_text(encoding="utf-8")
    source = source.split('if clean.limit(1).count() == 0:')[0]
    source = source.replace('spark.sql("CREATE SCHEMA IF NOT EXISTS silver")', '')
    source = source.replace('Files/bronze_taxi/', tmp_path.as_posix() + '/')
    source = source.replace('end_date = ""', 'end_date = "2024-02-29"')
    namespace = {"spark": spark}
    exec(compile(source, "NB_Silver_Taxi", "exec"), namespace)
    rows = namespace["clean"].collect()
    assert len(rows) == 3
    assert sum(row.total_amount_usd for row in rows) == Decimal("44.00")
    assert {row.pickup_location_id for row in rows} == {1, 264}
