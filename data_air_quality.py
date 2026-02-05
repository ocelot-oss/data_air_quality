import requests
import pandas as pd
import io
import json
from datetime import datetime, timedelta
from urllib.parse import quote

# === CONFIG ===
STATIONS_CSV = "stations.csv"
OUTPUT_GEOJSON = "air_data_gouv.geojson"
WANTED_POLLUTANTS = []  # vide = tous

def build_e2_url(date: datetime):
    """
    Construire l'URL de téléchargement via l'API MinIO
    """
    date_str = date.strftime("%Y-%m-%d")
    year_str = date.strftime("%Y")
    
    # Chemin du fichier (sans encoding ici, on l'encode après)
    file_path = f"lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{year_str}/FR_E2_{date_str}.csv"
    
    # URL encodée
    file_path_encoded = quote(file_path, safe='')
    
    return f"https://object.infra.data.gouv.fr/api/v1/buckets/ineris-prod/objects/download?prefix={file_path_encoded}"

def download_csv(url):
    """
    Télécharge le fichier et retourne un DataFrame
    """
    print(f"Téléchargement : {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        
        print(f"Status: {r.status_code}")
        print(f"Content-Type: {r.headers.get('Content-Type')}")
        print(f"Taille: {len(r.content)} octets")
        
        if r.status_code == 200 and len(r.content) > 100:
            # Afficher aperçu
            print("=== APERÇU (200 premiers caractères) ===")
            print(r.text[:200])
            print("=========================================")
            
            # Parser le CSV
            try:
                df = pd.read_csv(io.StringIO(r.text), sep=";")
                
                if df.empty:
                    print("⚠️ Vide avec sep=';', test avec ','")
                    df = pd.read_csv(io.StringIO(r.text), sep=",")
                
                print(f"✅ CSV parsé : {len(df)} lignes, {len(df.columns)} colonnes")
                if not df.empty:
                    print("Premières colonnes :", df.columns.tolist()[:5])
                return df
            except Exception as e:
                print(f"❌ Erreur parsing CSV : {e}")
                return pd.DataFrame()
        else:
            print(f"❌ Fichier vide ou erreur HTTP")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Erreur requête : {e}")
        return pd.DataFrame()

# ============ LOGIQUE PRINCIPALE ============

# Chercher le fichier le plus récent
target_date = datetime.utcnow().date() - timedelta(days=1)
print(f"\n🔍 Recherche du fichier pour le {target_date}")

df_measures = download_csv(build_e2_url(datetime.combine(target_date, datetime.min.time())))

# Retry sur les jours précédents si nécessaire
tries = 5
i = 1
while df_measures.empty and i < tries:
    test_date = target_date - timedelta(days=i)
    print(f"\n🔍 Tentative avec {test_date}")
    df_measures = download_csv(build_e2_url(datetime.combine(test_date, datetime.min.time())))
    i += 1

if df_measures.empty:
    print("\n❌ Aucun fichier E2 valide trouvé sur les 5 derniers jours.")
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
try:
    df_stations = pd.read_csv(STATIONS_CSV, sep=";")
    print(f"Stations chargées : {len(df_stations)} lignes")
    print(f"Colonnes stations : {df_stations.columns.tolist()[:5]}")
except Exception as e:
    print(f"❌ Erreur lecture stations.csv : {e}")
    exit(1)

# Merge mesures + coords
print("\n🔗 Merge des données...")
df_merged = df_measures.merge(
    df_stations,
    left_on="code site",
    right_on="Code",
    how="left"
)

if df_merged.empty:
    print("❌ Merge a échoué - aucune correspondance")
    exit(1)

# Filtrer les lignes sans coordonnées
df_merged = df_merged[df_merged['Latitude'].notna() & df_merged['Longitude'].notna()]
print(f"✅ Merge réussi : {len(df_merged)} lignes avec coordonnées")

# Création GeoJSON
print("\n🗺️  Création du GeoJSON...")
features = []

for _, row in df_merged.iterrows():
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(row["Longitude"]), float(row["Latitude"])]
        },
        "properties": {
            "code_station": str(row.get("code site", "")),
            "nom_station": str(row.get("Nom site", "")),
            "polluant": str(row.get("Polluant", "")),
            "date": str(row.get("Date de début", "") or row.get("Date", "")),
            "concentration": float(row.get("valeur", 0)) if pd.notna(row.get("valeur")) else None,
            "unite": str(row.get("Unité de mesure", "")),
        }
    })

geojson = {"type": "FeatureCollection", "features": features}

with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False, indent=2)

print(f"✅ GeoJSON généré : {OUTPUT_GEOJSON} ({len(features)} points)")













