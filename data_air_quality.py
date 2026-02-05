import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta

# === CONFIG ===

# Base URL du stockage des fichiers E2
BASE_DATA_URL = (
    "https://object.infra.data.gouv.fr/"
    "ineris-prod/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"
)

STATIONS_CSV_LOCAL = "stations.csv"   # ton fichier local de stations
OUTPUT_GEOJSON = "air_data_gouv.geojson"

# Polluants que tu veux inclure (facultatif, sinon laisse vide)
WANTED_POLLUTANTS = []  # [] = toutes

# === FONCTIONS UTILES ===

def build_e2_url_for_date(date: datetime):
    """
    Construit l'URL du fichier E2 correspondant à la date donnée.
    Par convention, les fichiers s'appellent FR_E2_YYYY-MM-DD.csv
    et sont dans un sous‑répertoire 'YYYY'.
    """
    date_str = date.strftime("%Y-%m-%d")
    year_str = date.strftime("%Y")
    filename = f"FR_E2_{date_str}.csv"
    return f"{BASE_DATA_URL}{year_str}/{filename}"

def download_e2_csv(url):
    """Télécharge et parse le CSV si disponible."""
    print(f"Téléchargement du fichier E2 : {url}")
    r = requests.get(url)
    if r.status_code == 200 and len(r.text) > 100:
        try:
            df = pd.read_csv(io.StringIO(r.text), sep=";")
            print(f"CSV chargé ({len(df)} lignes).")
            return df
        except Exception as e:
            print("Erreur lecture CSV :", e)
            return pd.DataFrame()
    else:
        print(f"Fichier non trouvé ou vide (status {r.status_code}).")
        return pd.DataFrame()

# === SCRIPT PRINCIPAL ===

# 1) Déterminer la date cible (hier)
target_date = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
url_e2 = build_e2_url_for_date(target_date)

df_measures = download_e2_csv(url_e2)

if df_measures.empty:
    print("Aucun fichier E2 valide trouvé pour la date cible.")
    exit(1)

# 2) Filtrer polluants si demandé
if WANTED_POLLUTANTS:
    df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]

if df_measures.empty:
    print("Aucune mesure après filtrage des polluants.")
    exit(1)

# 3) Lire le fichier local des stations (coordonnées)
df_stations = pd.read_csv(STATIONS_CSV_LOCAL, sep=";")

# Normaliser la colonne du code de station pour merger
df_stations = df_stations.rename(columns={"Code": "code_station"})

# 4) Fusionner mesures + coordonnées
df_merged = df_measures.merge(
    df_stations,
    left_on="CodeStation",
    right_on="code_station",
    how="left"
)

if df_merged.empty:
    print("Aucune correspondance entre mesures et stations.")
    exit(1)

# 5) Générer GeoJSON
features = []
for _, row in df_merged.iterrows():
    lon = row.get("Longitude")
    lat = row.get("Latitude")
    if pd.notna(lon) and pd.notna(lat):
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lon, lat],
            },
            "properties": {
                "code_station": row["CodeStation"],
                "polluant": row["Polluant"],
                "date": row["Date"],
                "concentration": row.get("Concentration"),
                "nom_station": row.get("Nom station"),
                "commune": row.get("Commune"),
            }
        })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("✅ GeoJSON généré avec succès !")












