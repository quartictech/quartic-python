
import pandas as pd
from quartic.analysis.geo import transform, df_to_geojson
import numpy as np
import math
from datetime import datetime

def test_transform():
    df = pd.DataFrame({"easting": [100, 200, 300],
                       "northing": [400, 500, 600], "other": ["wat", None, 3]})
    backup = df.copy(True)
    out = transform(df)
    lat = pd.Series([49.770454, 49.771416, 49.772378])
    assert np.isclose(lat, out["lat"]).all()
    lon = pd.Series([-7.556188, -7.554909, -7.553629])
    assert np.isclose(lon, out["lon"]).all()

    # Shouldn't touch original dataframe
    assert df.equals(backup)

def test_df_to_geojson():
    df = pd.DataFrame({
        "idx": [0, 1, 2],
        "lat": [51.1111, None, 52],
        "lon": [0.5, None, 1.0],
        "foo": [None, "ladispute", 1],
        "datetime": pd.to_datetime([datetime(2013, 1, 1), datetime(2014, 1, 1), datetime(2015, 1, 1)])
        })
    g = df_to_geojson(df, "lat", "lon", id_col="idx")

    assert g["type"] == "FeatureCollection"

    for idx, f in enumerate(g["features"]):
        assert f["id"] == df.idx[idx]
        assert f["properties"]["foo"] == df.foo[idx]
        assert datetime.fromtimestamp(f["properties"]["datetime"] / 1000) == df.datetime[idx]
        assert f["geometry"] == None if pd.isnull(df.lon[idx]) or pd.isnull(df.lat[idx]) else {"type": "Point",
                                                                                               "coordinates": [df.lon[idx], df.lat[idx]]}

def test_df_to_geojson_handles_inf():
    df = pd.DataFrame({
        "lat": [52],
        "lon": [1.0],
        "weird": [np.inf]
        })
    g = df_to_geojson(df, "lat", "lon")

    f = g["features"][0]
    assert f["properties"] == {}

def test_df_to_geojson_handles_nan():
    df = pd.DataFrame({
        "lat": [52],
        "lon": [1.0],
        "weird": [np.nan]
        })
    g = df_to_geojson(df, "lat", "lon")

    f = g["features"][0]
    assert f["properties"] == {}

def test_df_to_geojson_handles_nat():
    df = pd.DataFrame({
        "lat": [52],
        "lon": [1.0],
        "datetime": [pd.NaT]
        })
    g = df_to_geojson(df, "lat", "lon")

    f = g["features"][0]
    assert f["properties"] == {}

def test_df_to_geojson_handles_nan_coords():
    df = pd.DataFrame({
        "lat": [math.nan],
        "lon": [1.0],
        })
    g = df_to_geojson(df, "lat", "lon")

    f = g["features"][0]
    assert f["geometry"] is None


