from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("total, zone, error", [(33, 1, None), (34, 1, "revenue differs"), (33, 2, "Missing ZoneID")])
def test_gold_checks_detect_invalid_results(spark, total, zone, error):
    views = {
        "DimDate": "SELECT DATE '2024-01-07' AS DateID",
        "DimZone": "SELECT 1 AS ZoneID",
        "DimGDP": "SELECT 2024 AS year",
        "DimFX": "SELECT DATE '2024-01-07' AS DateID, DATE '2024-01-05' AS SourceRateDate",
        "FactTaxiDaily": f"SELECT DATE '2024-01-07' AS DateID, {zone} AS ZoneID, 2 AS TotalTrips, {total} AS TotalAmount, 30 AS TotalFareAmount, 0.9 AS EurPerUsdRate",
        "FactAirQualityDaily": "SELECT DATE '2024-01-07' AS DateID, 1 AS ZoneID",
        "silver_taxi": "SELECT DATE '2024-01-07' AS pickup_date, 1 AS pickup_location_id, total AS total_amount_usd, fare AS fare_amount FROM VALUES (11,10), (22,20) AS t(total,fare)",
    }
    for name, query in views.items():
        spark.sql(f"CREATE OR REPLACE TEMP VIEW {name} AS {query}")
    written = []

    class Summary:
        @property
        def write(self):
            return self

        def format(self, value):
            return self

        def mode(self, value):
            return self

        def saveAsTable(self, name):
            written.append(name)

    class Tables:
        def table(self, name):
            return spark.table(name.replace("gold.", "").replace("silver.", ""))

        def sql(self, query):
            return spark.sql(query.replace("gold.", "").replace("silver.", ""))

        def createDataFrame(self, data, schema):
            assert len(data) == 6
            return Summary()

    source = (ROOT / "NB_Gold_Checks.Notebook/notebook-content.py").read_text(encoding="utf-8")
    if error:
        with pytest.raises(ValueError, match=error):
            exec(compile(source, "gold_checks", "exec"), {"spark": Tables()})
        assert written == []
    else:
        exec(compile(source, "gold_checks", "exec"), {"spark": Tables()})
        assert written == ["gold.DataQualitySummary"]
