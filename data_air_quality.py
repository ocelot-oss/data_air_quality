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
    """
    Construire l'URL du CSV E2 pour une date donnée
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
    print(f"Téléchargement : {url}")
    
    # Headers pour imiter un navigateur
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    r = requests.get(url, headers=headers, timeout=30)
    
    print(f"Status: {r.status_code}")
    print(f"Content-Type: {r.headers.get('Content-Type')}")
    print(f"Taille: {len(r.content)} octets")
    
    if r.status_code == 200:
        # Afficher aperçu du contenu
        print("=== APERÇU (300 premiers caractères) ===")
        print(r.text[:300])
        print("=========================================")
        
        # Vérifier si c'est bien un CSV et pas du HTML
        if len(r.text) > 100:
            try:
                # Essayer d'abord avec séparateur ;
                df = pd.read_csv(io.StringIO(r.text), sep=";")
                
                if df.empty:
                    print("⚠️ Vide avec sep=';', test avec ','")
                    df = pd.read_csv(io.StringIO(r.text), sep=",")
                
                print(f"✅ CSV parsé : {len(df)} lignes, {len(df.columns)} colonnes")
                if not df.empty:
                    print("Colonnes :", df.columns.tolist()[:10])  # Afficher max 10 colonnes
                return df
            except Exception as e:
                print(f"❌ Erreur lecture CSV : {e}")
                return pd.DataFrame()
        else:
            print("❌ Contenu trop court (< 100 caractères)")
    else:
        print(f"❌ Erreur HTTP {r.status_code}")
    
    return pd.DataFrame()

# ============ LOGIQUE PRINCIPALE ============

# Commencer avec aujourd'hui (2026-02-05) pour tester
target_date = datetime(2026, 2, 5).date()
print(f"\n🔍 Recherche du fichier pour le {target_date}")

df_measures = download_csv(build_e2_url(datetime.combine(target_date, datetime.min.time())))

# Si vide, essayer les jours précédents (jusqu'à 3 jours)
tries = 3
i = 1
while df_measures.empty and i < tries:
    test_date = target_date - timedelta(days=i)
    print(f"\n🔍 Tentative avec {test_date}")
    df_measures = download_csv(build_e2_url(datetime.combine(test_date, datetime.min.time())))
    i += 1

if df_measures.empty:
    print("\n❌ Aucun fichier E2 valide trouvé sur les derniers jours.")
    exit(1)

print("\n✅ Fichier de mesures chargé !")
print(f"Colonnes disponibles : {df_measures.columns.tolist()}")

# Filtrer polluants si spécifié
if WANTED_POLLUTANTS:
    df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]
    print(f"Filtrage polluants : {len(df_measures)} lignes restantes")

if df_measures.empty:
    print("❌ Aucune donnée après filtrage des polluants")
    exit(1)

# Lire stations locales
print(f"\n📍 Chargement du fichier stations : {STATIONS_CSV}")
df_stations = pd.read_csv(STATIONS_CSV, sep=";")
print(f"Colonnes stations : {df_stations.columns.tolist()}")

# Merge mesures + coords (AVANT de renommer)
print("\n🔗 Merge des données...")
df_merged = df_measures.merge(
    df_stations,
    left_on="code site",  # Ajuste selon le vrai nom de colonne
    right_on="Code",      # Ajuste selon le vrai nom de colonne
    how="left"
)

if df_merged.empty:
    print("❌ Aucune correspondance entre mesures et stations.")
    print("Vérifiez que les colonnes 'code site' et 'Code' existent et matchent.")
    exit(1)

print(f"✅ Merge réussi : {len(df_merged)} lignes")

# Création GeoJSON
print("\n🗺️  Création du GeoJSON...")
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
                "code_station": str(row.get("code site", "")),
                "polluant": str(row.get("Polluant", "")),
                "date": str(row.get("Date de début", "") or row.get("Date", "")),
                "concentration": float(row.get("valeur", 0)) if pd.notna(row.get("valeur")) else None,
            }
        })

geojson = {"type": "FeatureCollection", "features": features}

with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print(f"✅ GeoJSON généré : {OUTPUT_GEOJSON} ({len(features)} points)")













