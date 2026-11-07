import pandas as pd
import pygeohash as pgh
from pathlib import Path

# Phoenix coordinates (our initial test area)
CENTER_LAT = 33.4484
CENTER_LON = -112.0740

# Generate a 5x5 grid around the center
OFFSETS = [-0.02, -0.01, 0, 0.01, 0.02]

# Geohash precision controls the geographic cell size
GEOHASH_PRECISION = 6

locations = []
location_number = 1

for lat_offset in OFFSETS:
    for lon_offset in OFFSETS:
        latitude = CENTER_LAT + lat_offset
        longitude = CENTER_LON + lon_offset

        geohash = pgh.encode(
            latitude,
            longitude,
            precision=GEOHASH_PRECISION
        )

        locations.append({
            "location_id": f"PHX_{location_number:03d}",
            "city": "Phoenix",
            "latitude": latitude,
            "longitude": longitude,
            "geohash": geohash,
            "geohash_precision": GEOHASH_PRECISION
        })

        location_number += 1

df = pd.DataFrame(locations)

# Save the catalog in the project's reference-data directory
project_root = Path(__file__).resolve().parents[1]
output_path = project_root / "data" / "reference" / "location_catalog.csv"

df.to_csv(output_path, index=False)

print("Total locations:", len(df))
print("Unique geohashes:", df["geohash"].nunique())
print("Duplicate location IDs:", df["location_id"].duplicated().sum())

print("\nCatalog preview:")
print(df.head(10))

print("\nSaved to:", output_path)
