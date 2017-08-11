import math
import sys
import datetime

try:
    import pyproj
    import numpy as np
    import pandas as pd
except ImportError:
    sys.stderr.write("failed to import a required module")
    raise

def transform(df, x_in_col="easting", y_in_col="northing", x_out_col="lon", y_out_col="lat",
              proj_src='epsg:27700', proj_dest='epsg:4326'):
    """ Perform coordinate transformation on columns in a DataFrame """
    crs_src = pyproj.Proj(init=proj_src)
    crs_dest = pyproj.Proj(init=proj_dest)

    transform_f = lambda row: pd.Series(
        pyproj.transform(crs_src, crs_dest, row[x_in_col], row[y_in_col]), dtype=np.float64
    )
    lat_lon = df.apply(transform_f, axis=1)
    new_df = df.copy()
    new_df[y_out_col] = lat_lon[1]
    new_df[x_out_col] = lat_lon[0]
    return new_df

def df_to_geojson_features(df, lat_col, lon_col, id_col=None):
    """ Create GeoJSON from a dataframe, extracting Point coordinates from specified columns """
    lat_col_idx = df.columns.tolist().index(lat_col)
    lon_col_idx = df.columns.tolist().index(lon_col)
    id_col_idx = df.columns.tolist().index(id_col) if id_col else None
    other_idx = [(idx, name) for idx, name in enumerate(df.columns.tolist())
                 if idx != lat_col_idx and idx != lon_col_idx]
    for row in df.iterrows():
        out = {"type": "Feature", "properties": {}}
        if id_col_idx:
            out["id"] = row[1][id_col_idx]

        for idx, key in other_idx:
            v = row[1][idx]
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                continue
            if v is pd.NaT:
                continue
            if isinstance(v, (pd.Timestamp, datetime.datetime)):
                v = (v - datetime.datetime(1970, 1, 1)).total_seconds() * 1000
            out["properties"][key] = v
        lon, lat = row[1][lon_col_idx], row[1][lat_col_idx]

        if pd.isnull(lon) or pd.isnull(lat):
            out["geometry"] = None
        else:
            out["geometry"] = {
                "type": "Point",
                "coordinates": [lon, lat]
            }
        yield out

def df_to_geojson(*args, **kwargs):
    features = list(df_to_geojson_features(*args, **kwargs))
    return {
        "type": "FeatureCollection",
        "features": features
    }

