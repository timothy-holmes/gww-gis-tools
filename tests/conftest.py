import json
import os

import pandas as pd
import pytest

try:
    import spark
except ImportError:
    in_databricks = False
else:
    in_databricks = True

try:
    import geopandas
except ImportError:
    has_geopandas = False
else:
    has_geopandas = True
    from shapely.geometry import shape


def to_gdf(df):
    if has_geopandas and "geometry" in df.columns:
        df["geometery"] = df["geometery"].apply(shape)
        gdf = geopandas.GeoDataFrame(df, geometry="geometery")
    else:
        gdf = df

    gdf.columns = gdf.columns.str.lower()
    return df


@pytest.fixture(scope="session")
def sample_data():
    test_mode = os.environ.get("TEST_MODE", "simple")

    if test_mode in ["simple", "local"]:
        with open(
            f"./tests/test_data/trace/{test_mode}/test_data.json", "r"
        ) as data_json:
            dfs = {
                k: to_gdf(pd.DataFrame(v)) for k, v in json.load(data_json).items() if v
            }
    elif test_mode == "databricks" and in_databricks:
        # pyspark queries for each table
        queries = {
            "pipes": """
                SELECT * FROM pipe_table
            """,
            # 'nodes': """""",
            # 'branches': """""",
            # 'parcels': """"""
        }
        dfs = {k: to_gdf(spark.query(v).toPandas()) for k, v in queries.items()}
    else:
        raise NotImplementedError(f"Test mode {test_mode} not implemented")

    return dfs
