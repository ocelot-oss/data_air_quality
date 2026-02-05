import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta

# === CONFIG ===
BASE_DATA_URL = (
    "https://object.infra.data.gouv.fr/"
    "ineris-prod/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"
)
STATIONS_CSV = "stations.csv"
OUTPUT_GEOJSON = "air_data_gouv.geojson"
WANTED_POLLUTANTS = []  # vide = tous

def build_e2_url(date: datetime):
    date_str = date.strftime("%Y-%m-%d")
    year_str = date.strftime("%Y")
    filename = f"FR_E2_{date_str}.csv"
    return f"{BASE_DATA_URL}{year_str}/{filename}"

def download_csv(url):
    print(f"Téléchargement : {url}")
    r = requests.get(url, timeout=30)
    if r.status_code == 200 and len(r.text) > 100:
        try:
            df = pd.read_csv(io.StringIO(r.text), sep=";")
            print(f"✅ CSV chargé : {len(df)} lignes")
            return df
        except Exception as e:
            print(f"❌ Erreur lecture CSV : {e}")
            return pd.DataFrame()
    else:
        print(f"❌ Fichier introuvable (status {r.status_code})")
        return pd.DataFrame()

# ============ LOGIQUE PRINCIPALE ============
target_date = datetime.utcnow().date() - timedelta(days=1)
df_measures = download_csv(build_e2_url(datetime.combine(target_date, datetime.min.time())))

# Retry logic
tries = 3
i = 1
while df_measures.empty and i < tries:
    test_date = target_date - timedelta(days=i)
    df_measures = download_csv(build_e2_url(datetime.combine(test_date, datetime.min.time())))
    i += 1

if df_measures.empty:
    print("❌ Aucun fichier E2 valide trouvé")
    exit(1)

# Debug : afficher les colonnes disponibles
print("Colonnes mesures :", df_measures.columns.tolist())

# Filtrer polluants
if WANTED_POLLUTANTS:
    df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]

if df_measures.empty:
    print("❌ Aucune donnée après filtrage polluants")
    exit(1)

# Lire stations SANS renommer avant le merge
df_stations = pd.read_csv(STATIONS_CSV, sep=";")
print("Colonnes stations :", df_stations.columns.tolist())

# Merge AVANT de renommer
df_merged = df_measures.merge(
    df_stations,
    left_on="code site",  # colonne dans df_measures
    right_on="Code",       # colonne dans df_stations (AVANT rename)
    how="left"
)

if df_merged.empty:
    print("❌ Merge a échoué")
    exit(1)

print(f"✅ Merge réussi : {len(df_merged)} lignes")

# Création GeoJSON
features = []
for _, row in df_merged.iterrows():
    lon = row.get("Longitude")
    lat = row.get("Latitude")
    
    if pd.notna(lon) and pd.notna(lat):
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(lon), float(lat)]
            },
            "properties": {
                "code_station": str(row.get("code site", "")),  # ou row.get("Code")
                "polluant": str(row.get("Polluant", "")),
                "date": str(row.get("Date de début", "") or row.get("Date", "")),
                "concentration": float(row.get("valeur", 0)) if pd.notna(row.get("valeur")) else None,
            }
        })

geojson = {"type": "FeatureCollection", "features": features}

with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print(f"✅ GeoJSON généré : {OUTPUT_GEOJSON} ({len(features)} points)")












