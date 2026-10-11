import ast
import calendar
import io
import json
from datetime import date, datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import pytest


ROOT = Path(__file__).resolve().parents[1]


def functions(name):
    source = (ROOT / f"{name}.Notebook/notebook-content.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    definitions = [node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name != "_request_json"]
    return compile(ast.Module(body=definitions, type_ignores=[]), name, "exec")


@pytest.mark.parametrize("today, expected", [(date(2026, 1, 5), date(2025, 11, 30)), (date(2024, 4, 1), date(2024, 2, 29)), (date(2026, 10, 11), date(2026, 8, 31))])
def test_default_period_crosses_year_and_leap_year(today, expected):
    class Clock:
        @staticmethod
        def now(tz):
            return datetime.combine(today, datetime.min.time(), tz)

    source = (ROOT / "NB_Bronze_Taxi.Notebook/notebook-content.py").read_text(encoding="utf-8")
    window = source[source.index("today ="):source.index('folder =')]
    namespace = {"datetime": Clock, "date": date, "timezone": timezone, "calendar": calendar, "start_date": "2024-01-01", "end_date": ""}
    exec(window, namespace)
    assert namespace["last_date"] == expected


def test_openaq_keeps_local_date_and_quality_metadata():
    response = {"results": [{
        "value": 0, "coverage": {"percentCoverage": 80}, "flagInfo": {"hasFlags": True},
        "period": {"datetimeFrom": {"utc": "2024-01-08T01:00:00Z", "local": "2024-01-07T20:00:00-05:00"}, "datetimeTo": {"utc": "2024-01-09T01:00:00Z"}},
    }]}
    namespace = {"datetime": datetime, "page_limit": 1000, "query_from": "from", "query_to": "to", "first_date": date(2024, 1, 7), "last_date": date(2024, 1, 7), "_request_json": lambda path, query: response}
    exec(functions("NB_Bronze_OpenAQ"), namespace)
    rows = namespace["_fetch_sensor_days"](1)
    assert rows[0]["measurement_date"] == date(2024, 1, 7)
    assert rows[0]["value"] == 0.0
    assert rows[0]["coverage_percent"] == 80.0
    assert rows[0]["has_flags"] is True


def test_openaq_paginates_and_filters_local_dates():
    calls = []

    def request(path, query):
        calls.append(query)
        if query["page"] == "1":
            return {"results": [{"value": 2, "period": {"datetimeFrom": {"utc": "2024-01-07T05:00:00Z", "local": "2024-01-07T00:00:00-05:00"}}}]}
        return {"results": []}

    namespace = {"datetime": datetime, "page_limit": 1, "query_from": "from", "query_to": "to", "first_date": date(2024, 1, 8), "last_date": date(2024, 1, 8), "_request_json": request}
    exec(functions("NB_Bronze_OpenAQ"), namespace)
    assert namespace["_fetch_sensor_days"](1) == []
    assert len(calls) == 2
    assert calls[0]["date_to"] == "to"


def test_pipeline_references_and_notebook_parameters():
    items = {}
    for path in ROOT.rglob(".platform"):
        platform = json.loads(path.read_text(encoding="utf-8"))
        items[platform["config"]["logicalId"]] = path.parent
    for path in ROOT.glob("*.DataPipeline/pipeline-content.json"):
        pipeline = json.loads(path.read_text(encoding="utf-8"))["properties"]
        names = {activity["name"] for activity in pipeline["activities"]}
        for activity in pipeline["activities"]:
            for dependency in activity["dependsOn"]:
                assert dependency["activity"] in names
            props = activity["typeProperties"]
            for key in ("notebookId", "dataflowId", "pipelineId"):
                if key in props:
                    assert props[key] in items
            if activity["type"] == "TridentNotebook":
                source = (items[props["notebookId"]] / "notebook-content.py").read_text(encoding="utf-8")
                for parameter in props.get("parameters", {}):
                    assert f"{parameter} = " in source
            if activity["type"] == "InvokePipeline":
                child = json.loads((items[props["pipelineId"]] / "pipeline-content.json").read_text(encoding="utf-8"))["properties"]
                assert set(props["parameters"]) <= set(child.get("parameters", {}))


@pytest.mark.parametrize("coverage, expected", [(100, True), (50, False), (None, False)])
def test_openaq_does_not_replace_tables_without_usable_measurements(monkeypatch, coverage, expected):
    monkeypatch.setitem(__import__("sys").modules, "notebookutils", SimpleNamespace())
    monkeypatch.setattr("time.sleep", lambda seconds: None)

    def response(request, timeout):
        query = parse_qs(urlsplit(request.full_url).query)
        if "/locations?" in request.full_url:
            results = [{"id": 100, "coordinates": {"latitude": 40.7, "longitude": -74.0}, "sensors": [{"id": 1, "parameter": {"name": "pm25", "displayName": None, "units": "µg/m³"}}]}] if query["page"] == ["1"] else []
        else:
            results = [{"value": 10, "coverage": {"percentCoverage": coverage}, "flagInfo": {"hasFlags": False}, "period": {"datetimeFrom": {"utc": "2024-01-07T05:00:00Z", "local": "2024-01-07T00:00:00-05:00"}, "datetimeTo": {"utc": "2024-01-08T05:00:00Z"}}}]
        return io.BytesIO(json.dumps({"results": results}).encode())

    monkeypatch.setattr("urllib.request.urlopen", response)
    writes = []

    class Frame:
        @property
        def write(self):
            return self

        def format(self, value):
            return self

        def mode(self, value):
            return self

        def option(self, *args):
            return self

        def saveAsTable(self, name):
            writes.append(name)

    spark = SimpleNamespace(sql=lambda query: None, createDataFrame=lambda data, schema: Frame())
    source = (ROOT / "NB_Bronze_OpenAQ.Notebook/notebook-content.py").read_text(encoding="utf-8")
    source = source.replace('openaq_api_key = ""', 'openaq_api_key = "test-key"').replace('end_date = ""', 'end_date = "2024-01-31"')
    if expected:
        exec(compile(source, "bronze_openaq", "exec"), {"spark": spark})
        assert set(writes) == {"dbo.raw_openaq_full", "dbo.raw_openaq_locations"}
    else:
        with pytest.raises(RuntimeError, match="no usable measurements"):
            exec(compile(source, "bronze_openaq", "exec"), {"spark": spark})
        assert writes == []
