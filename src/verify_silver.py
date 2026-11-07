import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "environment_observations"
)

connection = duckdb.connect()

parquet_path = str(SILVER_PATH / "**" / "*.parquet")

# 1. Count records
print("\n--- RECORD COUNT ---")

print(
    connection.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet('{parquet_path}', hive_partitioning=true)
        """
    ).fetchall()
)

# 2. Inspect the schema
print("\n--- SCHEMA ---")

print(
    connection.execute(
        f"""
        DESCRIBE
        SELECT *
        FROM read_parquet('{parquet_path}', hive_partitioning=true)
        """
    ).df()
)

# 3. Inspect environmental measurements
print("\n--- ENVIRONMENTAL DATA ---")

print(
    connection.execute(
        f"""
        SELECT
            location_id,
            geohash,
            observation_timestamp,
            heat_index_celsius,
            apparent_temperature_celsius,
            relative_humidity_percent,
            precipitation_mm
        FROM read_parquet('{parquet_path}', hive_partitioning=true)
        """
    ).df()
)

# 4. Check duplicates
print("\n--- DUPLICATE CHECK ---")

print(
    connection.execute(
        f"""
        SELECT
            location_id,
            observation_timestamp,
            COUNT(*) AS record_count
        FROM read_parquet('{parquet_path}', hive_partitioning=true)
        GROUP BY location_id, observation_timestamp
        HAVING COUNT(*) > 1
        """
    ).df()
)

connection.close()