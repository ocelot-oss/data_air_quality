import requests
import pandas as pd
import io
import json
import re
from datetime import datetime
from bs4 import BeautifulSoup

# === CONFIG ===

# dossier object storage où sont publiés les fichiers temps réel
BASE_DATA_URL = "https://object.infra.data.gouv.fr/ineris-prod/lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/"

STATIONS_CSV_LOCAL = "stations.csv"   # ton fichier local
OUTPUT_GEOJSON = "air_data_gouv.geojson"

# filtre des polluants que tu veux (par exemple)
WANTED_POLLUTANTS = ["NO2","PM10","O3","PM2.5","SO2","CO"]

# === FONCTIONS UTILES ===

def find_latest_data_file():
    """
    Récupère la liste des fichiers disponibles sur la page object storage
    et renvoie l'URL du fichier le plus récent.
    """

    r = requests.get(BASE_DATA_URL)
    soup = BeautifulSoup(r.text, "html.parser")

    # trouver tous les liens vers des fichiers CSV
    links = [a["href"] for a in soup.find_all("a", href=True) if a["href"].endswith(".csv")]

    # si aucun fichier trouvé
    if not links:
        print("Aucun fichier CSV trouvé dans", BASE_DATA_URL)
        return None

    # déterminer le plus récent par nom (souvent format YYYYMMDD dans le nom)
    links_sorted = sorted(links, reverse=True)
    latest_file = links_sorted[0]
    return BASE_DATA_URL + latest_file

def download_csv(url):
    """
    Télécharge le CSV depuis l'URL et retourne un DataFrame pandas.
    """
    print(f"Téléchargement du fichier : {url}")
    r = requests.get(url)
    r.raise_for_status()
    return pd.read_csv(io.StringIO(r.text), sep=";")

# === SCRIPT PRINCIPAL ===

# 1) trouver le fichier le plus récent
latest_url = find_latest_data_file()
if not latest_url:
    print("Impossible de récupérer un fichier temporal des mesures.")
    exit(1)

df_measures = download_csv(latest_url)
print(f"Lignes mesurées : {len(df_measures)}")

# filtrer polluants si nécessaire
df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]

if df_measures.empty:
    print("Aucune mesure filtrée pour les polluants demandés.")
    exit(1)

# 2) lire les stations locales
df_stations = pd.read_csv(STATIONS_CSV_LOCAL, sep=";")

# on s'assure que la colonne "Code" de stations est alignée avec le code station des mesures
df_stations = df_stations.rename(columns={"Code": "code_station"})

# 3) fusionner
df = df_measures.merge(
    df_stations,
    left_on="CodeStation",
    right_on="code_station",
    how="left"
)

if df.empty:
    print("Merge vide entre mesures et stations.")
    exit(1)

# 4) générer GeoJSON
features = []
for _, row in df.iterrows():
    lon = row.get("Longitude")
    lat = row.get("Latitude")
    if pd.notna(lon) and pd.notna(lat):
        features.append({
            "type": "Feature",
            "geometry": { "type": "Point", "coordinates": [lon, lat] },
            "properties": {
                "code_station": row["CodeStation"],
                "polluant": row["Polluant"],
                "date": row["Date"],
                "concentration": row["Concentration"],
                "nom_station": row.get("Nom station"),
                "commune": row.get("Commune")
            }
        })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("GeoJSON généré avec succès !")













