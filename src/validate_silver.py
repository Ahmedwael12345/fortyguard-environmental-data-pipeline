from pathlib import Path
import duckdb
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "silver"
    / "environment_observations"
)

parquet_files = list(SILVER_PATH.rglob("*.parquet"))

if not parquet_files:
    raise SystemExit("VALIDATION FAILED: No Silver Parquet files found.")

connection = duckdb.connect()

try:
    parquet_path = str(SILVER_PATH / "**" / "*.parquet").replace("'", "''")

    record_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{parquet_path}',
            hive_partitioning = true
        )
        """
    ).fetchone()[0]

    if record_count == 0:
        raise ValueError("Silver dataset is empty.")

    duplicate_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM (
            SELECT location_id, observation_timestamp
            FROM read_parquet(
                '{parquet_path}',
                hive_partitioning = true
            )
            GROUP BY location_id, observation_timestamp
            HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]

    null_key_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{parquet_path}',
            hive_partitioning = true
        )
        WHERE location_id IS NULL
           OR geohash IS NULL
           OR observation_timestamp IS NULL
           OR source_activity_id IS NULL
        """
    ).fetchone()[0]

    invalid_measurement_count = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM read_parquet(
            '{parquet_path}',
            hive_partitioning = true
        )
        WHERE relative_humidity_percent < 0
           OR relative_humidity_percent > 100
           OR precipitation_mm < 0
        """
    ).fetchone()[0]

    print("\n--- SILVER DATA QUALITY REPORT ---")
    print(f"Records: {record_count}")
    print(f"Duplicate observations: {duplicate_count}")
    print(f"Rows with null key fields: {null_key_count}")
    print(f"Rows with invalid measurements: {invalid_measurement_count}")

    if duplicate_count or null_key_count or invalid_measurement_count:
        raise ValueError("Silver data quality checks failed.")

    print("\nVALIDATION PASSED")

except Exception as error:
    print(f"\nVALIDATION FAILED: {error}")
    sys.exit(1)

finally:
    connection.close()
