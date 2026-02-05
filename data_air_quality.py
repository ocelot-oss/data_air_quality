import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta

# === CONFIGURATION ===

API_DATA_URL = "https://data.opendatasoft.com/api/records/1.0/search/"
STATIONS_CSV_URL = "https://www.geodair.fr/api-ext/stations/export"
POLLUTANT = "NO2"     # par exemple NO2, PM10, O3...
START_DATE = "2026-02-01"  # format yyyy-mm-dd
END_DATE = "2026-02-02"
MAX_ROWS = 10000       # nb max de lignes à récupérer

# === 1) Récupérer les mesures via l’API ods/data.gouv.fr ===

params = {
    "dataset": "donnees-temps-reel-de-mesure-des-concentrations-de-polluants-atmospheriques-reglementes-1",
    "refine.polluant": POLLUTANT,
    "refine.date": START_DATE,
    "rows": MAX_ROWS
}

print(f"📥 Récupération des mesures du polluant {POLLUTANT} pour {START_DATE}…")
r = requests.get(API_DATA_URL, params=params)
r.raise_for_status()
data = r.json()

# Normaliser en DataFrame
records = pd.json_normalize(data.get("records", []))
print(f"Lignes de mesures reçues : {len(records)}")

if len(records) == 0:
    print("🚫 Aucune mesure trouvée pour cette période — vérifie les paramètres.")
    exit()

df_measures = pd.DataFrame({
    "code_station": records["fields.code_station"],
    "polluant": records["fields.polluant"],
    "date": pd.to_datetime(records["fields.date"]),
    "concentration": records["fields.concentration"]
})


# === 2) Récupérer les coordonnées des stations ===

print("📥 Téléchargement des stations (coordonnées)...")
r2 = requests.get(STATIONS_CSV_URL, headers={"apikey": ""})  # clé API Geod’air si nécessaire
r2.encoding = 'utf-8'

df_stations = pd.read_csv(io.StringIO(r2.text), sep=";")
print(f"Lignes stations récupérées : {len(df_stations)}")

# Garder uniquement les colonnes utiles
df_stations = df_stations[["Code", "Longitude", "Latitude", "Nom station", "Commune"]]


# === 3) Merge mesures + stations ===

print("🔗 Fusion des mesures et des coordonnées des stations…")
df_merged = df_measures.merge(
    df_stations,
    left_on="code_station",
    right_on="Code",
    how="left"
)

print(f"Lignes après merge : {len(df_merged)}")


# === 4) Générer GeoJSON ===

print("🌍 Création du GeoJSON…")
features = []
for _, row in df_merged.iterrows():
    if pd.notna(row["Longitude"]) and pd.notna(row["Latitude"]):
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["Longitude"], row["Latitude"]],
            },
            "properties": {
                "code_station": row["code_station"],
                "nom_station": row["Nom station"],
                "commune": row["Commune"],
                "date": row["date"].strftime("%Y-%m-%d %H:%M:%S"),
                "polluant": row["polluant"],
                "concentration": row["concentration"]
            }
        })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open("air_data_gouv.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("✅ GeoJSON généré avec succès !")














