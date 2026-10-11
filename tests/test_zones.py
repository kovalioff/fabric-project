from pathlib import Path
from types import SimpleNamespace
from zipfile import ZipFile

import geopandas as gpd
import pandas as pd
import pytest
from shapely.geometry import box


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("longitude, expected", [(0.5, 1), (0.0, 1), (5.0, None)])
def test_mapping_uses_lookup_csv_and_preserves_special_zones(tmp_path, monkeypatch, longitude, expected):
    polygons = gpd.GeoDataFrame({"LocationID": [1], "zone": ["Test"], "borough": ["Test"]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:4326")
    polygons.to_file(tmp_path / "taxi_zones.shp")
    with ZipFile(tmp_path / "taxi_zones.zip", "w") as archive:
        for path in tmp_path.glob("taxi_zones.*"):
            if path.suffix != ".zip":
                archive.write(path, path.name)
    (tmp_path / "taxi_zone_lookup.csv").write_text("LocationID,Borough,Zone,service_zone\n1,Test,Test,Yellow\n264,Unknown,N/A,N/A\n265,N/A,Outside of NYC,N/A\n", encoding="utf-8")
    monkeypatch.setattr("urllib.request.urlopen", lambda request, timeout: (tmp_path / request.full_url.rsplit("/", 1)[1]).open("rb"))
    stations = pd.DataFrame({"LocationID": [100], "latitude": [0.5], "longitude": [longitude]})
    writes = {}

    class Frame:
        def __init__(self, data):
            self.data = data
            self.write = self

        def select(self, *args):
            return self

        def distinct(self):
            return self

        def toPandas(self):
            return self.data

        def format(self, value):
            return self

        def mode(self, value):
            return self

        def option(self, *args):
            return self

        def saveAsTable(self, name):
            writes[name] = self.data

    spark = SimpleNamespace(sql=lambda query: None, read=SimpleNamespace(table=lambda name: Frame(stations)), createDataFrame=lambda data, schema=None: Frame(data))
    source = (ROOT / "NB_Silver_OpenAQ_ZoneMapping.Notebook/notebook-content.py").read_text(encoding="utf-8")
    source = "\n".join(line for line in source.splitlines() if not line.startswith("%pip"))
    if expected is None:
        with pytest.raises(ValueError, match="mapping is empty"):
            exec(compile(source, "zone_mapping", "exec"), {"spark": spark})
        assert writes == {}
    else:
        exec(compile(source, "zone_mapping", "exec"), {"spark": spark})
        assert writes["silver.silver_openaq_zone_mapping"] == [(100, expected)]
        reference = writes["silver.silver_taxi_zone_reference"]
        assert set(reference["LocationID"]) == {1, 264, 265}
        assert "service_zone" in reference
