import json
import os

import pytest


@pytest.fixture(scope="session")
def sample_data():
    test_mode = os.environ.get("TEST_MODE", "simple")

    if test_mode == "simple":
        asset_data = {"pipes": [], "nodes": [], "branches": [], "parcels": []}
    elif test_mode == "local":
        with open(r"tests\test_data\trace\test_pipes.geojson", "r") as data_json:
            asset_data = json.load(data_json)
    elif test_mode == "databricks":
        asset_data = "?"

    return asset_data
