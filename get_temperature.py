import os
import requests
from dotenv import load_dotenv
import json

load_dotenv()

API_KEY = os.getenv("FORTYGUARD_API_KEY")

url = "https://api.fortyguard.com/v1/env_params"

headers = {
    "api-key": API_KEY,
    "Content-Type": "application/json"
}

payload = {
    "latitude":33.4484,
    "longitude": -112.0740,
    "temperature": 30,
    "date_time": {
        "start_date": "2025-09-20",
        "start_time": "14:00",
        "filter_type": 1
    }
}

response = requests.post(
    url,
    headers=headers,
    json=payload,
    timeout=60
)

print("HTTP Status:", response.status_code)

try:
    result = response.json()
    print(json.dumps(result, indent=4))
except ValueError:
    print(response.text)