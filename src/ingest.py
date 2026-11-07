
import os
import json
import time
import requests
import pandas as pd

from pathlib import Path
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dotenv import load_dotenv


# --------------------------------------------------
# Configuration
# --------------------------------------------------

load_dotenv()

API_KEY = os.getenv("FORTYGUARD_API_KEY")
BASE_URL = "https://api.fortyguard.com"

PROJECT_ROOT = Path(__file__).resolve().parents[1]

CATALOG_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "location_catalog.csv"
)

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
)

QUARANTINE_PATH = (
    PROJECT_ROOT
    / "data"
    / "quarantine"
    / "api_failures"
)


# --------------------------------------------------
# Ingestion configuration
# --------------------------------------------------

# Available modes: "historical" or "hourly"
INGESTION_MODE = "hourly"

# Historical observation configuration
HISTORICAL_DATE = "2025-09-20"
HISTORICAL_TIME = "14:00"

# API reference temperature input.
# This is not an independent temperature measurement.
REFERENCE_TEMPERATURE = 35

# Development limit
TEST_LOCATION_COUNT = 25


# --------------------------------------------------
# Polling configuration
# --------------------------------------------------

POLL_INTERVAL_SECONDS = 5
MAX_POLL_ATTEMPTS = 20


# --------------------------------------------------
# Build API request payload
# --------------------------------------------------

def build_request_payload(
    latitude,
    longitude,
    location_timezone
):

    if INGESTION_MODE == "historical":

        observation_time = datetime.strptime(
            f"{HISTORICAL_DATE} {HISTORICAL_TIME}",
            "%Y-%m-%d %H:%M"
        )

    elif INGESTION_MODE == "hourly":

        local_timezone = ZoneInfo(location_timezone)

        current_local_time = datetime.now(
            local_timezone
        )

        # Request the latest completed hour
        observation_time = (
            current_local_time
            .replace(
                minute=0,
                second=0,
                microsecond=0
            )
            - timedelta(hours=1)
        )

    else:

        raise ValueError(
            f"Unsupported ingestion mode: {INGESTION_MODE}"
        )

    payload = {

        "latitude": float(latitude),

        "longitude": float(longitude),

        "temperature": REFERENCE_TEMPERATURE,

        "date_time": {

            "start_date": observation_time.strftime(
                "%Y-%m-%d"
            ),

            "start_time": observation_time.strftime(
                "%H:%M"
            ),

            "filter_type": 1
        }
    }

    return payload


# --------------------------------------------------
# Submit API request
# --------------------------------------------------

def request_environment_data(session, payload):

    url = f"{BASE_URL}/v1/env_params"

    headers = {

        "api-key": API_KEY,

        "Content-Type": "application/json"
    }

    print("\nSubmitting API request:")
    print(json.dumps(payload, indent=4))

    try:

        response = session.post(
            url,
            headers=headers,
            json=payload,
            timeout=60
        )

        print("HTTP status:", response.status_code)

        response.raise_for_status()

        request_result = response.json()

    except requests.exceptions.Timeout as error:

        raise RuntimeError(
            "FortyGuard API request timed out"
        ) from error

    except requests.exceptions.HTTPError as error:

        raise RuntimeError(
            f"FortyGuard API returned an HTTP error: {error}"
        ) from error

    except requests.exceptions.RequestException as error:

        raise RuntimeError(
            f"Could not connect to FortyGuard API: {error}"
        ) from error

    except ValueError as error:

        raise RuntimeError(
            "FortyGuard returned invalid JSON"
        ) from error

    return request_result


# --------------------------------------------------
# Poll asynchronous API activity
# --------------------------------------------------

def get_activity_result(session, activity_id):

    url = f"{BASE_URL}/v1/status/{activity_id}"

    headers = {
        "api-key": API_KEY
    }

    for attempt in range(
        1,
        MAX_POLL_ATTEMPTS + 1
    ):

        try:

            response = session.get(
                url,
                headers=headers,
                timeout=30
            )

            response.raise_for_status()

            status_response = response.json()

        except requests.exceptions.RequestException as error:

            raise RuntimeError(
                f"Error polling activity {activity_id}: {error}"
            ) from error

        except ValueError as error:

            raise RuntimeError(
                "Invalid JSON returned by status endpoint"
            ) from error

        data = status_response.get("data", {})

        status = str(
            data.get("status", "")
        ).lower()

        print(
            f"Polling attempt {attempt}/"
            f"{MAX_POLL_ATTEMPTS}: "
            f"{status or 'unknown'}"
        )

        if status in ["completed", "succeeded"]:

            result = data.get("result")

            if result is None:

                raise ValueError(
                    "Activity completed but no result was returned"
                )

            return result

        if status in ["failed", "error"]:

            raise RuntimeError(
                f"API activity failed: {status_response}"
            )

        time.sleep(POLL_INTERVAL_SECONDS)

    raise TimeoutError(
        f"Activity {activity_id} did not finish"
    )


# --------------------------------------------------
# Save successful response in Bronze
# --------------------------------------------------

def save_bronze_record(
    location_id,
    activity_id,
    request_payload,
    initial_response,
    final_result
):

    BRONZE_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    raw_record = {

        "location_id": location_id,

        "request_timestamp_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "request": request_payload,

        "initial_response": initial_response,

        "result": final_result
    }

    output_file = (
        BRONZE_PATH
        / f"{location_id}_{activity_id}.json"
    )

    # Never overwrite raw records
    with open(output_file, "x") as file:

        json.dump(
            raw_record,
            file,
            indent=4
        )

    print("\nBronze record saved:", output_file)


# --------------------------------------------------
# Save failed requests in Quarantine
# --------------------------------------------------

def save_failure_record(
    location_id,
    request_payload,
    error,
    activity_id=None
):

    QUARANTINE_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    failure_record = {

        "location_id": location_id,

        "failed_at_utc": datetime.now(
            timezone.utc
        ).isoformat(),

        "activity_id": activity_id,

        "request": request_payload,

        "error_type": type(error).__name__,

        "error_message": str(error)
    }

    filename = (
        f"{location_id}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}.json"
    )

    output_file = QUARANTINE_PATH / filename

    with open(output_file, "x") as file:

        json.dump(
            failure_record,
            file,
            indent=4
        )

    print(
        "Failure saved to quarantine:",
        output_file
    )


# --------------------------------------------------
# Process one location
# --------------------------------------------------

def process_location(session, location):

    location_id = location["location_id"]

    activity_id = None
    request_payload = None

    try:

        # Build request
        request_payload = build_request_payload(
            latitude=location["latitude"],
            longitude=location["longitude"],
            location_timezone=location["timezone"]
        )

        # Submit request
        request_result = request_environment_data(
            session,
            request_payload
        )

        # Extract activity ID
        activity_id = (
            request_result
            .get("data", {})
            .get("activity_id")
        )

        if not activity_id:

            raise ValueError(
                "Activity ID missing from initial response"
            )

        print("Activity ID:", activity_id)

        # Wait for completion
        final_result = get_activity_result(
            session,
            activity_id
        )

        # Save Bronze record
        save_bronze_record(
            location_id=location_id,
            activity_id=activity_id,
            request_payload=request_payload,
            initial_response=request_result,
            final_result=final_result
        )

        return True

    except Exception as error:

        print(f"\nFAILED: {location_id}")
        print("Error:", error)

        try:

            save_failure_record(
                location_id=location_id,
                request_payload=request_payload,
                error=error,
                activity_id=activity_id
            )

        except OSError as quarantine_error:

            print(
                "Could not save failure record:",
                quarantine_error
            )

        return False


# --------------------------------------------------
# Main ingestion process
# --------------------------------------------------

def main():

    # Validate API key
    if not API_KEY:

        raise ValueError(
            "FORTYGUARD_API_KEY is missing from .env"
        )

    # Validate catalog
    if not CATALOG_PATH.exists():

        raise FileNotFoundError(
            f"Location catalog not found: {CATALOG_PATH}"
        )

    locations = pd.read_csv(
        CATALOG_PATH
    )

    required_columns = [

        "location_id",

        "latitude",

        "longitude",

        "timezone"
    ]

    missing_columns = [

        column
        for column in required_columns
        if column not in locations.columns
    ]

    if missing_columns:

        raise ValueError(
            f"Catalog is missing columns: {missing_columns}"
        )

    if locations[required_columns].isnull().any().any():

        raise ValueError(
            "Catalog contains null values in required columns"
        )

    # Validate timezone values before processing
    for timezone_name in locations["timezone"].unique():

        ZoneInfo(timezone_name)

    # Limit development ingestion
    locations = locations.head(
        TEST_LOCATION_COUNT
    )

    if locations.empty:

        raise ValueError(
            "No locations available for ingestion"
        )

    successful_count = 0
    failed_count = 0

    print("\nIngestion mode:", INGESTION_MODE)
    print("Locations:", len(locations))

    # Reuse HTTP session
    with requests.Session() as session:

        for _, location in locations.iterrows():

            location_id = location["location_id"]

            print("\n" + "=" * 50)
            print("Processing:", location_id)
            print("=" * 50)

            success = process_location(
                session,
                location
            )

            if success:

                successful_count += 1

            else:

                failed_count += 1

    # Final summary
    print("\n" + "=" * 50)
    print("INGESTION SUMMARY")
    print("=" * 50)

    print("Total locations:", len(locations))
    print("Successful:", successful_count)
    print("Failed:", failed_count)


# --------------------------------------------------
# Entry point
# --------------------------------------------------

if __name__ == "__main__":

    main()
