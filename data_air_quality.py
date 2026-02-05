import requests
import pandas as pd
import io
import time
from datetime import datetime, timedelta
import json

API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"

# CSV des stations (coordonnées)
STATIONS_URL = "https://www.geodair.fr/api-ext/station/export"
STATISTIQUE_EXPORT_URL = "https://www.geodair.fr/api-ext/statistique/export"
STATISTIQUE_DOWNLOAD_URL = "https://www.geodair.fr/api-ext/download"

JOURS_MAX = 7  # Nombre maximum de jours à remonter si pas de mesures

# ------------------------------
# Fonctions utilitaires
# ------------------------------

def get_period(days=1):
    """
    Renvoie date_debut et date_fin pour la période demandée
    """
    fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    debut = fin - timedelta(days=days)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

def download_stations():
    print("Téléchargement des stations...")
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    r = requests.get(STATIONS_URL, headers=headers)
    df = pd.read_csv(io.StringIO(r.text), sep=";")
    df.columns = df.columns.str.strip()
    print(f"Lignes stations récupérées: {len(df)}")
    return df

def generate_stat_file(date_debut, date_fin, zas="FR84ZAR03", famille_polluant=2000, type_donnee="a1"):
    """
    Appelle l'API pour générer le fichier statistiques et retourne l'identifiant.
    """
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    params = {
        "zas": zas,
        "famille_polluant": famille_polluant,
        "date_debut": date_debut,
        "date_fin": date_fin,
        "type_donnee": type_donnee
    }
    r = requests.get(STATISTIQUE_EXPORT_URL, headers=headers, params=params)
    r.raise_for_status()
    file_id = r.text.strip()  # l'API renvoie l'identifiant du fichier
    print(f"ID du fichier généré: {file_id}")
    return file_id

def download_csv_from_id(file_id, max_attempts=10, delay=1):
    """
    Télécharge le CSV depuis l'API en utilisant l'identifiant généré.
    Attend que le CSV contienne réellement des mesures.
    """
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    
    for attempt in range(max_attempts):
        r = requests.get(STATISTIQUE_DOWNLOAD_URL, headers=headers, params={"id": file_id})
        text = r.text.strip()
        
        # Vérifier que le CSV contient plus qu'un simple ID
        if text.count("\n") > 1:
            try:
                df = pd.read_csv(io.StringIO(text), sep=";")
                df.columns = df.columns.str.strip()
                print(f"Lignes mesures récupérées: {len(df)}")
                return df
            except pd.errors.EmptyDataError:
                pass  # fichier encore incomplet
        
        print(f"Fichier non prêt, tentative {attempt+1}/{max_attempts}...")
        time.sleep(delay)
    
    print("Aucune mesure récupérée après plusieurs tentatives.")
    return pd.DataFrame()

# ------------------------------
# Script principal
# ------------------------------

stations = download_stations()

mesures = pd.DataFrame()
jours = 1
while jours <= JOURS_MAX and mesures.empty:
    date_debut, date_fin = get_period(days=jours)
    print(f"Tentative d'export pour la période {date_debut} -> {date_fin}...")
    
    file_id = generate_stat_file(date_debut, date_fin)
    mesures = download_csv_from_id(file_id)
    
    if mesures.empty:
        jours += 1

if mesures.empty:
    print("Aucune mesure disponible dans les derniers jours. GeoJSON non généré.")
    exit(0)

# Vérifier le nom exact des colonnes pour merge
# Exemple : 'Code station' ou 'Code'
if 'Code' in mesures.columns:
    merge_left = 'Code'
elif 'code_station' in mesures.columns:
    merge_left = 'code_station'
else:
    print("Impossible de trouver la colonne identifiant station dans mesures.")
    exit(1)

if 'Code' not in stations.columns:
    print("Impossible de trouver la colonne Code dans stations.")
    exit(1)

df = mesures.merge(stations, left_on=merge_left, right_on='Code', how='left')

# Créer le GeoJSON
features = []
for _, row in df.iterrows():
    if pd.notnull(row.get("Longitude")) and pd.notnull(row.get("Latitude")):
        feature = {
            "type": "Feature",
            "properties": {k: row[k] for k in df.columns if k not in ["Longitude", "Latitude"]},
            "geometry": {
                "type": "Point",
                "coordinates": [row["Longitude"], row["Latitude"]]
            }
        }
        features.append(feature)

geojson = {"type": "FeatureCollection", "features": features}

with open("air.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print(f"GeoJSON généré avec {len(features)} points.")











