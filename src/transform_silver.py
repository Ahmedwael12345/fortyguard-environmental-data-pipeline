
import json
import shutil
import pandas as pd

from pathlib import Path


# --------------------------------------------------
# Configuration
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
CATALOG_PATH = PROJECT_ROOT / "data" / "reference" / "location_catalog.csv"
SILVER_PATH = PROJECT_ROOT / "data" / "silver" / "environment_observations"


# --------------------------------------------------
# Helper: safely extract values
# --------------------------------------------------

def get_value(values, index):

    if isinstance(values, list):
        return values[index] if index < len(values) else None

    return values


# --------------------------------------------------
# Transform one Bronze JSON file
# --------------------------------------------------

def transform_file(file_path, geohash_lookup):

    with open(file_path, "r") as file:
        raw = json.load(file)

    result = raw["result"]

    timestamps = result["metadata"]["timestamps"]
    timezone = result["metadata"]["timezone"]

    records = []

    for location in result["locations"]:

        location_id = raw["location_id"]

        parameters = location.get("parameters", {})

        solar = location.get("solar_irradiance", {})
        clear_sky = solar.get("clear_sky", {})

        for index, timestamp in enumerate(timestamps):

            record = {
                "location_id": location_id,
                "geohash": geohash_lookup.get(location_id),
                "latitude": location["lat"],
                "longitude": location["lon"],
                "elevation_m": location.get("elevation"),
                "observation_timestamp": timestamp,
                "timezone": timezone,

                "reference_temperature_celsius": location.get("temperature"),

                "heat_index_celsius": get_value(
                    parameters.get("heat_index_celsius", []), index
                ),

                "apparent_temperature_celsius": get_value(
                    parameters.get("apparent_temperature_celsius", []), index
                ),

                "relative_humidity_percent": get_value(
                    parameters.get("relative_humidity_percent", []), index
                ),

                "precipitation_mm": get_value(
                    parameters.get("precipitation_mm", []), index
                ),

                "cloud_cover_octas": get_value(
                    parameters.get("cloud_cover_octas", []), index
                ),

                "wet_bulb_temperature_celsius": get_value(
                    parameters.get("wet_bulb_temperature_celsius", []), index
                ),

                "air_quality_index": get_value(
                    parameters.get("air_quality:idx", []), index
                ),

                "pm2_5_index": get_value(
                    parameters.get("air_quality_pm2p5:idx", []), index
                ),

                "pm10_index": get_value(
                    parameters.get("air_quality_pm10:idx", []), index
                ),

                "no2_index": get_value(
                    parameters.get("air_quality_no2:idx", []), index
                ),

                "co2_ppm": get_value(
                    parameters.get("co2_ppm", []), index
                ),

                "methane_ppb": get_value(
                    parameters.get("methane_ppb", []), index
                ),

                "solar_ghi_w_m2": get_value(clear_sky.get("ghi"), index),
                "solar_dni_w_m2": get_value(clear_sky.get("dni"), index),
                "solar_dhi_w_m2": get_value(clear_sky.get("dhi"), index),

                "source_activity_id": (
                    raw["initial_response"]["data"]["activity_id"]
                ),

                # Used to choose the latest successful ingestion
                "ingested_at_utc": raw["request_timestamp_utc"]
            }

            records.append(record)

    return records


# --------------------------------------------------
# Deduplicate observations
# --------------------------------------------------

def deduplicate_data(df):

    original_count = len(df)

    # Convert ingestion timestamps to UTC
    df["ingested_at_utc"] = pd.to_datetime(
        df["ingested_at_utc"],
        utc=True,
        errors="coerce"
    )

    if df["ingested_at_utc"].isna().any():
        raise ValueError("Invalid or missing ingestion timestamps")

    # Sort oldest to newest
    df = df.sort_values(
        "ingested_at_utc",
        ascending=True
    )

    # Keep the latest ingestion for each location and observation time
    df = df.drop_duplicates(
        subset=["location_id", "observation_timestamp"],
        keep="last"
    )

    removed_count = original_count - len(df)

    print("\nDeduplication completed!")
    print("Records before:", original_count)
    print("Duplicates removed:", removed_count)
    print("Records after:", len(df))

    return df


# --------------------------------------------------
# Data quality checks
# --------------------------------------------------

def validate_data(df):

    required_columns = [
        "location_id",
        "geohash",
        "observation_timestamp",
        "latitude",
        "longitude",
        "source_activity_id"
    ]

    for column in required_columns:

        if df[column].isna().any():
            raise ValueError(f"Missing values found in {column}")

    if not df["latitude"].between(-90, 90).all():
        raise ValueError("Invalid latitude values")

    if not df["longitude"].between(-180, 180).all():
        raise ValueError("Invalid longitude values")

    humidity = df["relative_humidity_percent"].dropna()

    if not humidity.between(0, 100).all():
        raise ValueError("Humidity values outside 0-100")

    if df["precipitation_mm"].dropna().lt(0).any():
        raise ValueError("Negative precipitation values found")

    if df.duplicated(
        subset=["location_id", "observation_timestamp"]
    ).any():
        raise ValueError("Duplicate location/timestamp records found")


# --------------------------------------------------
# Main process
# --------------------------------------------------

def main():

    catalog = pd.read_csv(CATALOG_PATH)

    geohash_lookup = dict(
        zip(catalog["location_id"], catalog["geohash"])
    )

    json_files = sorted(BRONZE_PATH.glob("*.json"))

    if not json_files:
        raise FileNotFoundError("No Bronze JSON files found")

    # 1. Transform all Bronze records
    records = []

    for file_path in json_files:
        print(f"Processing: {file_path.name}")

        records.extend(
            transform_file(file_path, geohash_lookup)
        )

    new_df = pd.DataFrame(records)

    # 2. Load existing Silver records
    if SILVER_PATH.exists():
        print("\nLoading existing Silver records...")

        existing_df = pd.read_parquet(SILVER_PATH)

        print("Existing records:", len(existing_df))

        # Combine old and new records
        df = pd.concat(
            [existing_df, new_df],
            ignore_index=True
        )

    else:
        df = new_df

    print("Combined records before deduplication:", len(df))

    # 3. Standardize timestamps
    df["observation_timestamp"] = pd.to_datetime(
        df["observation_timestamp"],
        utc=True
    )

    # 4. Keep the latest ingestion per observation
    df = deduplicate_data(df)

    # 5. Recalculate observation date
    df["observation_date"] = (
        df["observation_timestamp"].dt.strftime("%Y-%m-%d")
    )

    # 6. Validate the combined dataset
    validate_data(df)

    # 7. Write to a temporary location first
    temporary_path = SILVER_PATH.with_name(
        "environment_observations_tmp"
    )

    backup_path = SILVER_PATH.with_name(
        "environment_observations_backup"
    )

    if temporary_path.exists():
        shutil.rmtree(temporary_path)

    if backup_path.exists():
        raise FileExistsError(
            f"Backup already exists: {backup_path}. "
            "Inspect it before continuing."
        )

    temporary_path.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    df.to_parquet(
        temporary_path,
        engine="pyarrow",
        index=False,
        partition_cols=["observation_date"]
    )

    # 8. Replace Silver only after the new dataset is written
    if SILVER_PATH.exists():
        SILVER_PATH.rename(backup_path)

    try:
        temporary_path.rename(SILVER_PATH)

    except Exception:
        if backup_path.exists():
            backup_path.rename(SILVER_PATH)
        raise

    # 9. Remove backup after successful replacement
    if backup_path.exists():
        shutil.rmtree(backup_path)

    print("\nIncremental Silver transformation completed!")
    print("Total records:", len(df))
    print("Unique locations:", df["location_id"].nunique())
    print("Unique geohashes:", df["geohash"].nunique())

    print("\nDataset preview:")
    print(df.head().to_string(index=False))

    print("\nSaved to:", SILVER_PATH)


if __name__ == "__main__":
    main()