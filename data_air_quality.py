import csv
import json
import requests

CSV_URL = "https://www.data.gouv.fr/api/1/datasets/r/157ceed4-ce03-4c7d-9cd7-ae60ea07417b"
OUTPUT_FILE = "air.geojson"

response = requests.get(CSV_URL)
response.raise_for_status()

lines = response.text.splitlines()
reader = csv.DictReader(lines)

features = []

for row in reader:
    try:
        lat = float(row["latitude"])
        lon = float(row["longitude"])
        value = row.get("valeur", "")
        pollutant = row.get("polluant", "")
        date = row.get("date", "")
    except (KeyError, ValueError):
        continue

    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [lon, lat]
        },
        "properties": {
            "polluant": pollutant,
            "valeur": value,
            "date": date
        }
    })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

print(f"{len(features)} points écrits")
