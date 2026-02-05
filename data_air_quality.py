#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import io
from datetime import datetime, timedelta
import json
import time

# --- CONFIGURATION ---
API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"
STATIONS_URL = "https://www.geodair.fr/api-ext/station/export"
STATISTIQUE_EXPORT_URL = "https://www.geodair.fr/api-ext/statistique/export"
STATISTIQUE_DOWNLOAD_URL = "https://www.geodair.fr/api-ext/download"
ZAS = "FR84ZAR03"
FAMILLE_POLLUANT = "2000"
TYPE_DONNEE = "a1"
GEOJSON_FILE = "air.geojson"
JOURS_MAX = 7  # nombre maximum de jours à vérifier si aucune mesure

# --- FONCTIONS UTILES ---
def get_period_single_day(offset_days=1):
    """
    Renvoie date_debut et date_fin pour un jour précis.
    offset_days=1 => hier, 2 => avant-hier, etc.
    """
    fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=offset_days-1)
    debut = fin - timedelta(days=1)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

def download_stations():
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    r = requests.get(STATIONS_URL, headers=headers)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), sep=";")
    df.columns = df.columns.str.strip().str.replace('"','')
    return df

def request_export(date_debut, date_fin):
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    params = {
        "zas": ZAS,
        "famille_polluant": FAMILLE_POLLUANT,
        "date_debut": date_debut,
        "date_fin": date_fin,
        "type_donnee": TYPE_DONNEE
    }
    r = requests.get(STATISTIQUE_EXPORT_URL, headers=headers, params=params)
    r.raise_for_status()
    return r.text.strip()  # C'est l'ID du fichier

def download_csv_from_id(file_id):
    headers = {"apikey": API_KEY, "accept": "text/csv; charset=UTF-8"}
    # L'API peut mettre quelques secondes à générer le fichier
    for attempt in range(10):
        r = requests.get(STATISTIQUE_DOWNLOAD_URL, headers=headers, params={"id": file_id})
        if r.headers.get("content-length") != "143":  # 143 = CSV vide / pas prêt
            df = pd.read_csv(io.StringIO(r.text), sep=";")
            df.columns = df.columns.str.strip().str.replace('"','')
            return df
        time.sleep(1)
    return pd.DataFrame()  # aucune mesure récupérée après 10 essais

# --- 1️⃣ Télécharger les stations ---
print("Téléchargement des stations...")
stations = download_stations()
print(f"Lignes stations récupérées: {len(stations)}")

# --- 2️⃣ Télécharger les mesures pour les derniers jours disponibles ---
dfs = []
for offset in range(1, JOURS_MAX + 1):
    date_debut, date_fin = get_period_single_day(offset_days=offset)
    print(f"Tentative d'export pour la période {date_debut} -> {date_fin}...")
    
    try:
        file_id = request_export(date_debut, date_fin)
    except Exception as e:
        print(f"Erreur lors de la demande d'export: {e}")
        continue

    if not file_id:
        print("Aucun ID retourné, passage au jour précédent")
        continue
    
    df_temp = download_csv_from_id(file_id)
    if not df_temp.empty:
        print(f"{len(df_temp)} mesures récupérées pour {date_debut}")
        dfs.append(df_temp)
        break  # On s'arrête dès qu'on trouve des mesures
    else:
        print("Aucune mesure trouvée, passage au jour précédent")

# Concaténer tous les DataFrames disponibles
df_mesures = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# --- 3️⃣ Merge avec stations ---
if df_mesures.empty:
    print("Aucune mesure disponible. GeoJSON non généré.")
else:
    df_mesures['Code'] = df_mesures['Code'].astype(str)
    stations['Code'] = stations['Code'].astype(str)
    
    df = df_mesures.merge(
        stations[['Code', 'Longitude', 'Latitude']],
        left_on='Code',
        right_on='Code',
        how='left'
    )

    # --- 4️⃣ Générer GeoJSON ---
    features = []
    for _, row in df.iterrows():
        if pd.notnull(row['Longitude']) and pd.notnull(row['Latitude']):
            feature = {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row['Longitude'], row['Latitude']]
                },
                "properties": row.drop(['Longitude','Latitude']).to_dict()
            }
            features.append(feature)

    geojson = {"type": "FeatureCollection", "features": features}
    
    with open(GEOJSON_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"GeoJSON généré : {GEOJSON_FILE} avec {len(features)} points")










