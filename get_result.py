import os
import requests
import time
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("FORTYGUARD_API_KEY")

# Our existing task ID
activity_id = "a18392bd-5cf3-4003-b480-2f0872a64428"

# Status endpoint
url = f"https://api.fortyguard.com/v1/status/{activity_id}"

headers = {
    "api-key": API_KEY
}

# Check task status
for attempt in range(24):

    response = requests.get(
        url,
        headers=headers,
        timeout=30
    )

    print("HTTP Status:", response.status_code)

    result = response.json()

    if response.status_code == 404:
        print("Activity not available yet. Retrying...")
        time.sleep(5)
        continue

    if response.status_code == 429:
        print("Rate limited. Wait before retrying.")
        break

    if response.status_code != 200:
        print(json.dumps(result, indent=4))
        break

    task_data = result.get("data", {})
    status = task_data.get("status", "").lower()

    print("Task status:", status)

    if status in ("completed", "succeeded"):

        print("\nTask completed!")

        actual_data = task_data.get("result")

        print(json.dumps(actual_data, indent=4))

        break

    elif status in ("failed", "error"):

        print("Task failed:")
        print(json.dumps(result, indent=4))
        break

    else:
        print("Still processing...")
        time.sleep(5)

else:
    print("Polling limit reached. Check the activity again later.")