import pandas as pd
import pytest

from src.transform_silver import validate_data


def create_valid_dataframe():
    return pd.DataFrame({
        "location_id": ["PHX_001"],
        "source_activity_id": ["test-activity-001"],
        "geohash": ["9tbq1j"],
        "observation_timestamp": [
            pd.Timestamp("2025-09-20 21:00:00", tz="UTC")
        ],
        "latitude": [33.4284],
        "longitude": [-112.094],
        "relative_humidity_percent": [25.3],
        "precipitation_mm": [0.0]
    })


def test_valid_data_passes():
    df = create_valid_dataframe()

    validate_data(df)


def test_missing_geohash_fails():
    df = create_valid_dataframe()
    df.loc[0, "geohash"] = None

    with pytest.raises(ValueError, match="geohash"):
        validate_data(df)


def test_invalid_humidity_fails():
    df = create_valid_dataframe()
    df.loc[0, "relative_humidity_percent"] = 110

    with pytest.raises(ValueError, match="Humidity"):
        validate_data(df)


def test_negative_precipitation_fails():
    df = create_valid_dataframe()
    df.loc[0, "precipitation_mm"] = -5

    with pytest.raises(ValueError, match="precipitation"):
        validate_data(df)


def test_duplicate_observation_fails():
    df = create_valid_dataframe()
    df = pd.concat([df, df], ignore_index=True)

    with pytest.raises(ValueError, match="Duplicate"):
        validate_data(df)