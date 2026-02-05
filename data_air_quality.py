import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta

# === CONFIG ===

# base URL des fichiers E2
BASE_DATA_URL = (
    "https://object.infra.data.gouv.fr/"
    "ineris-prod/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"
)

STATIONS_CSV = "stations.csv"        # ton fichier local
OUTPUT_GEOJSON = "air_data_gouv.geojson"

# polluants que tu veux garder (vide = tous)
WANTED_POLLUTANTS = []

def build_e2_url(date: datetime):
    """
    Construire l’URL du CSV E2 pour une date donnée
    en supposant le schéma FR_E2_YYYY-MM-DD.csv
    """
    date_str = date.strftime("%Y-%m-%d")
    year_str = date.strftime("%Y")
    filename = f"FR_E2_{date_str}.csv"
    return f"{BASE_DATA_URL}{year_str}/{filename}"

def download_csv(url):
    """
    Télécharge le fichier et retourne un DataFrame
    """
    print(f"Téléchargement du fichier : {url}")
    r = requests.get(url)
    if r.status_code == 200 and len(r.text) > 100:
        try:
            df = pd.read_csv(io.StringIO(r.text), sep=";")
            print("CSV chargé :", len(df), "lignes")
            return df
        except Exception as e:
            print("Erreur lors de la lecture du CSV:", e)
            return pd.DataFrame()
    else:
        print("Fichier non trouvé ou trop petit (status", r.status_code, ")")
        return pd.DataFrame()

# ============ LOGIQUE PRINCIPALE ============

# on commence avec "hier" pour s'assurer que le fichier est complet
target_date = datetime.utcnow().date() - timedelta(days=1)
df_measures = download_csv(build_e2_url(datetime.combine(target_date, datetime.min.time())))

# si vide, tenter les jours précédents (jusqu'à 3 jours max)
tries = 3
i = 1
while df_measures.empty and i < tries:
    test_date = target_date - timedelta(days=i)
    df_measures = download_csv(build_e2_url(datetime.combine(test_date, datetime.min.time())))
    i += 1

if df_measures.empty:
    print("❌ Aucun fichier E2 valide trouvé sur les derniers jours.")
    exit(1)

# filtrer polluants si spécifié
if WANTED_POLLUTANTS:
    df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]

if df_measures.empty:
    print("❌ Aucune donnée après filtrage des polluants")
    exit(1)

# lire stations locales
df_stations = pd.read_csv(STATIONS_CSV, sep=";")
df_stations = df_stations.rename(columns={"Code": "code_station"})

# merge mesures + coords
df_merged = df_measures.merge(
    df_stations,
    left_on="code site",
    right_on="Code",
    how="left"
)

if df_merged.empty:
    print("❌ Aucune correspondance entre mesures et stations.")
    exit(1)

# création GeoJSON
features = []
for _, row in df_merged.iterrows():
    lon = row.get("Longitude")
    lat = row.get("Latitude")
    if pd.notna(lon) and pd.notna(lat):
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [lon, lat]},
            "properties": {
                "code_station": row["CodeStation"],
                "polluant": row["Polluant"],
                "date": row.get("Date") or row.get("Date/Heure") or "",
                "concentration": row.get("Concentration"),
            }
        })

geojson = {"type": "FeatureCollection", "features": features}
with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("✅ GeoJSON généré :", OUTPUT_GEOJSON)













