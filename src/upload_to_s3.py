import boto3
from pathlib import Path
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[1]

BUCKET_NAME = "fortyguard-temperature-catalog-ahmed-2026"
REGION = "eu-north-1"

S3 = boto3.client("s3", region_name=REGION)

FINAL_PREFIX = "silver/environment_observations/"
TEMP_PREFIX = f"silver/_staging/{uuid4().hex}/"


def upload_file(local_path, s3_key):
    S3.upload_file(
        str(local_path),
        BUCKET_NAME,
        s3_key,
        ExtraArgs={"ServerSideEncryption": "AES256"}
    )
    print(f"Uploaded: {s3_key}")


def list_objects(prefix):
    paginator = S3.get_paginator("list_objects_v2")
    objects = []

    for page in paginator.paginate(
        Bucket=BUCKET_NAME,
        Prefix=prefix
    ):
        objects.extend(page.get("Contents", []))

    return objects


def delete_prefix(prefix):
    objects = list_objects(prefix)

    for start in range(0, len(objects), 1000):
        batch = objects[start:start + 1000]

        if batch:
            S3.delete_objects(
                Bucket=BUCKET_NAME,
                Delete={
                    "Objects": [
                        {"Key": obj["Key"]} for obj in batch
                    ],
                    "Quiet": True
                }
            )


def main():
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    silver_dir = (
        PROJECT_ROOT
        / "data"
        / "silver"
        / "environment_observations"
    )
    catalog = (
        PROJECT_ROOT
        / "data"
        / "reference"
        / "location_catalog.csv"
    )

    silver_files = list(silver_dir.rglob("*.parquet"))

    if not silver_files:
        raise RuntimeError("No Silver Parquet files found.")

    # 1. Upload Bronze
    bronze_files = list(bronze_dir.glob("*.json"))

    for file in bronze_files:
        upload_file(
            file,
            f"bronze/raw_api_responses/{file.name}"
        )

    # 2. Upload new Silver files to staging
    try:
        for file in silver_files:
            relative_path = file.relative_to(silver_dir)

            upload_file(
                file,
                f"{TEMP_PREFIX}{relative_path.as_posix()}"
            )

        staged_objects = list_objects(TEMP_PREFIX)

        if len(staged_objects) != len(silver_files):
            raise RuntimeError(
                f"Staging count mismatch: "
                f"{len(staged_objects)} uploaded, "
                f"{len(silver_files)} expected."
            )

        # 3. Remove previous Silver files only after staging succeeds
        old_objects = list_objects(FINAL_PREFIX)

        # 4. Copy staged files to final location
        for obj in staged_objects:
            relative_key = obj["Key"][len(TEMP_PREFIX):]

            S3.copy_object(
                Bucket=BUCKET_NAME,
                Key=f"{FINAL_PREFIX}{relative_key}",
                CopySource={
                    "Bucket": BUCKET_NAME,
                    "Key": obj["Key"]
                },
                ServerSideEncryption="AES256"
            )

        # Verify every expected file exists in the final dataset
        final_objects = list_objects(FINAL_PREFIX)

        expected_keys = {
            f"{FINAL_PREFIX}{file.relative_to(silver_dir).as_posix()}"
            for file in silver_files
        }

        actual_keys = {
            obj["Key"] for obj in final_objects
        }

        if not expected_keys.issubset(actual_keys):
            raise RuntimeError("Final Silver dataset is incomplete.")

        # Remove obsolete files only after successful publication
        stale_keys = actual_keys - expected_keys

        for start in range(0, len(stale_keys), 1000):
            batch = list(stale_keys)[start:start + 1000]

            if batch:
                S3.delete_objects(
                    Bucket=BUCKET_NAME,
                    Delete={
                        "Objects": [{"Key": key} for key in batch],
                        "Quiet": True
                    }
                )

        # Remove staging files
        delete_prefix(TEMP_PREFIX)

        print(f"Silver synchronized: {len(final_objects)} files")

    except Exception:
        print("Silver upload failed. Staging data retained for inspection.")
        raise

    # 6. Upload location catalog
    if catalog.exists():
        upload_file(catalog, "reference/location_catalog.csv")

    print("\nS3 upload completed successfully.")


if __name__ == "__main__":
    main()
