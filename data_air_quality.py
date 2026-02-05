import requests
import io
import pandas as pd
import json
from datetime import datetime, timedelta

# --- CONFIGURATION ---
API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"
ZAS = "FR84ZAR03"
FAMILLE_POLLUANT = 2000
TYPE_DONNEE = "a1"

# Chemin local du CSV stations
STATIONS_CSV = "stations.csv"  # soit téléchargé manuellement ou via API

# --- FONCTION DE CALCUL DE LA PÉRIODE ---
def get_period(days=1):
    """
    Renvoie date_debut et date_fin pour la période demandée
    """
    fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    debut = fin - timedelta(days=days)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

# --- FONCTION DE TÉLÉCHARGEMENT DU CSV DE MESURES ---
def download_measurements(date_debut, date_fin):
    url = (
        "https://www.geodair.fr/api-ext/statistique/export"
        f"?zas={ZAS}&famille_polluant={FAMILLE_POLLUANT}"
        f"&date_debut={date_debut}&date_fin={date_fin}&type_donnee={TYPE_DONNEE}"
    )
    headers = {"accept": "text/csv; charset=UTF-8", "apikey": API_KEY}
    
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Erreur API : {response.status_code}")
    
    # Le CSV est renvoyé directement
    df = pd.read_csv(io.StringIO(response.text), sep=";")
    return df

# --- CHARGEMENT DU CSV STATIONS ---
stations = pd.read_csv(STATIONS_CSV, sep=";", encoding="utf-8")

# --- CALCUL DE LA PÉRIODE ---
date_debut, date_fin = get_period(days=1)
print(f"Tentative d'export pour la période {date_debut} -> {date_fin}")

# --- TÉLÉCHARGEMENT DES MESURES ---
df_mesures = download_measurements(date_debut, date_fin)
print(f"Lignes récupérées: {len(df_mesures)}")

# --- JOINTURE POUR AJOUTER LONGITUDE ET LATITUDE ---
df = df_mesures.merge(
    stations[['Code', 'Longitude', 'Latitude']],
    left_on='Code',   # colonne dans df_mesures
    right_on='Code',          # colonne dans stations
    how='left'
)

# --- CRÉATION DU GEOJSON ---
features = []
for _, row in df.iterrows():
    if pd.notnull(row['Longitude']) and pd.notnull(row['Latitude']):
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row['Longitude'], row['Latitude']]
            },
            "properties": {
                "station": row.get("nom_station", ""),
                "polluant": row.get("polluant", ""),
                "valeur": row.get("valeur", None),
                "unite": row.get("unite", ""),
                "date": row.get("date_heure", "")
            }
        })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

# --- SAUVEGARDE DU GEOJSON ---
with open("air.geojson", "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print("GeoJSON généré : air.geojson")







