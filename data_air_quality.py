#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import io
from datetime import datetime, timedelta
import json

# --- CONFIGURATION ---
API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"
STATIONS_URL = "https://www.geodair.fr/api-ext/station/export"
STATISTIQUE_URL = "https://www.geodair.fr/api-ext/statistique/export"
ZAS = "FR84ZAR03"
FAMILLE_POLLUANT = "2000"
TYPE_DONNEE = "a1"
GEOJSON_FILE = "air.geojson"

# --- FONCTIONS UTILES ---
def get_period(days=1):
    """
    Renvoie date_debut et date_fin pour la période demandée.
    On prend jusqu'à hier minuit pour éviter données incomplètes.
    """
    fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    debut = fin - timedelta(days=days)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

def download_csv(url, params=None):
    """
    Télécharge un CSV depuis une URL ou un endpoint API, retourne un DataFrame pandas.
    """
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    df = pd.read_csv(io.StringIO(response.text), sep=";")
    # Nettoyage des colonnes : enlever guillemets et espaces
    df.columns = df.columns.str.strip().str.replace('"', '')
    return df

# --- 1️⃣ Télécharger les stations ---
print("Téléchargement des stations...")
stations = download_csv(STATIONS_URL)
print(f"Lignes stations récupérées: {len(stations)}")

# --- 2️⃣ Télécharger les mesures pour la période ---
date_debut, date_fin = get_period(days=1)
print(f"Tentative d'export pour la période {date_debut} -> {date_fin}...")

params = {
    "zas": ZAS,
    "famille_polluant": FAMILLE_POLLUANT,
    "date_debut": date_debut,
    "date_fin": date_fin,
    "type_donnee": TYPE_DONNEE
}

df_mesures = download_csv(STATISTIQUE_URL, params=params)
print(f"Lignes mesures récupérées: {len(df_mesures)}")

# --- 3️⃣ Si mesures disponibles, merge avec stations ---
if df_mesures.empty:
    print("Aucune mesure trouvée pour cette période. GeoJSON non généré.")
else:
    # Normaliser les colonnes Code pour merge
    df_mesures['Code'] = df_mesures['Code'].astype(str)
    stations['Code'] = stations['Code'].astype(str)
    
    # Merge pour ajouter Longitude / Latitude
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
    
    geojson = {
        "type": "FeatureCollection",
        "features": features
    }
    
    with open(GEOJSON_FILE, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)
    
    print(f"GeoJSON généré : {GEOJSON_FILE} avec {len(features)} points")







