import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta
import time

# === CONFIG ===
API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"
STATIONS_CSV_URL = "https://www.geodair.fr/api-ext/stations/export"
JOURS_MAX = 7        # nombre maximum de jours à vérifier si aucune donnée
SLEEP_BETWEEN_TRIES = 5  # secondes à attendre entre chaque tentative de téléchargement

# === FONCTIONS UTILES ===

def get_period(days=1, end=None):
    """
    Renvoie date_debut et date_fin pour la période demandée.
    Si end=None, prend hier comme date de fin pour éviter données incomplètes.
    """
    if end is None:
        fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    else:
        fin = end
    debut = fin - timedelta(days=days)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

def download_stations():
    print("Téléchargement des stations...")
    r = requests.get(STATIONS_CSV_URL, headers={"apikey": API_KEY})
    r.encoding = 'utf-8'
    df = pd.read_csv(io.StringIO(r.text), sep=";")
    print(f"Lignes stations récupérées: {len(df)}")
    return df

def generate_file_id(date_debut, date_fin):
    url = f"https://www.geodair.fr/api-ext/statistique/export?zas=FR84ZAR03&famille_polluant=2000&date_debut={date_debut}&date_fin={date_fin}&type_donnee=a2"
    r = requests.get(url, headers={"apikey": API_KEY})
    r.raise_for_status()
    file_id = r.text.strip()
    print("ID du fichier généré:", file_id)
    return file_id

def download_csv_from_id(file_id, max_tries=10):
    download_url = f"https://www.geodair.fr/api-ext/download?id={file_id}"
    for i in range(1, max_tries + 1):
        r = requests.get(download_url, headers={"apikey": API_KEY})
        r.encoding = 'utf-8'
        # Si le CSV contient plus de quelques caractères, on suppose qu'il est prêt
        if len(r.text) > 200:
            df = pd.read_csv(io.StringIO(r.text), sep=";")
            return df
        else:
            print(f"Fichier non prêt, tentative {i}/{max_tries}...")
            time.sleep(SLEEP_BETWEEN_TRIES)
    print("Aucune mesure récupérée après plusieurs tentatives.")
    return pd.DataFrame()  # retour vide si jamais rien

# === SCRIPT PRINCIPAL ===

stations = download_stations()
df_final = pd.DataFrame()

# Vérifier jusqu'à JOURS_MAX derniers jours pour trouver des mesures
for days_back in range(1, JOURS_MAX + 1):
    date_debut, date_fin = get_period(days=1, end=datetime.now() - timedelta(days=days_back))
    print(f"Tentative d'export pour la période {date_debut} -> {date_fin}...")

    file_id = generate_file_id(date_debut, date_fin)
    df_mesures = download_csv_from_id(file_id)

    if df_mesures.empty:
        print("Aucune mesure pour cette période.")
        continue

    # Vérifier la colonne identifiant station
    # Chercher une colonne commune : souvent "Code" ou "Code station"
    if "Code" in df_mesures.columns:
        merge_col_mesures = "Code"
    elif "Code station" in df_mesures.columns:
        merge_col_mesures = "Code station"
    else:
        print("Impossible de trouver la colonne identifiant station dans mesures.")
        continue

    df_merged = df_mesures.merge(
        stations,
        left_on=merge_col_mesures,
        right_on="Code",
        how="left"
    )

    if df_merged.empty:
        print("Merge vide, aucune correspondance avec les stations.")
        continue

    df_final = pd.concat([df_final, df_merged], ignore_index=True)
    break  # on prend la première période où il y a des mesures

if df_final.empty:
    print("Aucune mesure disponible dans les derniers jours. GeoJSON non généré.")
else:
    # Création GeoJSON
    features = []
    for _, row in df_final.iterrows():
        if pd.notnull(row.get("Longitude")) and pd.notnull(row.get("Latitude")):
            features.append({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [row["Longitude"], row["Latitude"]],
                },
                "properties": {col: row[col] for col in df_final.columns if col not in ["Longitude", "Latitude"]}
            })

    geojson = {
        "type": "FeatureCollection",
        "features": features
    }

    with open("air.geojson", "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print("GeoJSON généré avec succès !")












