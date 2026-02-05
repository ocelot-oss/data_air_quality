import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta

API_KEY = "ZjdpgWC8ZtAIWvBiZDKUFe7KRMBLorr0"  # Mets ta clé ici
BASE_URL = "https://www.geodair.fr/api-ext"
ZAS = "FR84ZAR03"
FAMILLE_POLLUANT = "2000"
TYPE_DONNEE = "a1"

# 1️⃣ Calcul des dates dynamiques
def get_period(days=1):
    """
    Renvoie date_debut et date_fin pour la période demandée
    """
    # date_fin = début du jour courant - 1 jour pour éviter données incomplètes
    fin = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    debut = fin - timedelta(days=days)
    return debut.strftime("%d/%m/%Y %H:%M"), fin.strftime("%d/%m/%Y %H:%M")

date_debut, date_fin = get_period(days=1)

# 2️⃣ Demande d'export CSV
params = {
    "zas": ZAS,
    "famille_polluant": FAMILLE_POLLUANT,
    "date_debut": date_debut,
    "date_fin": date_fin,
    "type_donnee": TYPE_DONNEE
}

headers = {
    "accept": "text/csv; charset=UTF-8",
    "apikey": API_KEY
}

print(f"Tentative d'export pour la période {date_debut} -> {date_fin}")
r = requests.get(f"{BASE_URL}/statistique/export", headers=headers, params=params)
r.raise_for_status()
file_id = r.text.strip()
print("ID du fichier généré:", file_id)

# 3️⃣ Téléchargement du fichier (réessaie si pas encore prêt)
def download_file(file_id, max_tries=10, wait_sec=2):
    url = f"{BASE_URL}/download"
    for i in range(max_tries):
        r = requests.get(url, headers={"apikey": API_KEY}, params={"id": file_id})
        if "text/csv" in r.headers.get("Content-Type", ""):
            return pd.read_csv(io.StringIO(r.text), sep=";")
        print(f"Fichier pas encore prêt, tentative {i+1}/{max_tries}...")
        import time
        time.sleep(wait_sec)
    raise RuntimeError("Le fichier n'a jamais été disponible.")

df = download_file(file_id)
print(f"Lignes récupérées: {len(df)}")

if len(df) == 0:
    print("Aucune donnée disponible pour cette période.")
else:
    # 4️⃣ Conversion en GeoJSON
    features = []
    for _, row in df.iterrows():
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": {
                "station": row["nom_station"],
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

    # 5️⃣ Sauvegarde GeoJSON
    out_file = f"geodair_{date_debut.replace('/', '-')}_{date_fin.replace('/', '-')}.geojson"
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(geojson, f, ensure_ascii=False, indent=2)

    print(f"GeoJSON créé: {out_file}")


