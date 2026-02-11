import requests
import pandas as pd
import io
import json
import chardet
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

# === CONFIG ===
STATIONS_CSV = "stations.csv"
OUTPUT_GEOJSON = "air_data_gouv.geojson"
WANTED_POLLUTANTS = []  # vide = tous, ou spécifie : ['NO2', 'PM10', 'O3']

def build_e2_url(date: datetime):
    """
    Construire l'URL de téléchargement via l'API MinIO
    """
    date_str = date.strftime("%Y-%m-%d")
    year_str = date.strftime("%Y")
    file_path = f"lcsqa/concentrations-de-polluants-atmospheriques-reglementes/temps-reel/{year_str}/FR_E2_{date_str}.csv"
    file_path_encoded = quote(file_path, safe='')
    return f"https://object.infra.data.gouv.fr/api/v1/buckets/ineris-prod/objects/download?prefix={file_path_encoded}"

def download_csv(url):
    """
    Télécharge le fichier et retourne un DataFrame
    avec détection automatique de l'encodage
    """
    print(f"Téléchargement : {url}")
    
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Accept': 'text/csv,application/csv,text/plain,*/*',
    }
    
    try:
        r = requests.get(url, headers=headers, timeout=30)
        print(f"Status: {r.status_code}")
        print(f"Taille: {len(r.content)} octets")
        
        if r.status_code == 200 and len(r.content) > 100:
            
            # 🔎 Détection automatique encodage
            detected = chardet.detect(r.content)
            encoding = detected['encoding']
            print(f"Encodage détecté : {encoding}")
            
            # Lecture propre avec encodage détecté
            df = pd.read_csv(
                io.BytesIO(r.content),
                sep=";",
                encoding=encoding,
                engine="python"
            )
            
            # Nettoyage simple des noms de colonnes
            df.columns = df.columns.str.strip()
            
            print(f"✅ CSV parsé : {len(df)} lignes, {len(df.columns)} colonnes")
            return df
        
        else:
            print("❌ Fichier vide ou erreur")
            return pd.DataFrame()
            
    except Exception as e:
        print(f"❌ Erreur : {e}")
        return pd.DataFrame()

# ============ LOGIQUE PRINCIPALE ============

# Chercher le fichier le plus récent
target_date = datetime.now(timezone.utc).date() - timedelta(days=1)
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

# FILTRE VALLÉE DE L'ARVE
print("\n🔍 Filtrage pour la vallée de l'Arve...")
df_measures = df_measures[
    df_measures['Zas'].str.contains('ARVE', case=False, na=False)
]

print(f"✅ {len(df_measures)} mesures trouvées")
print(f"Stations : {df_measures['nom site'].unique()}")
print(f"Polluants : {df_measures['Polluant'].unique()}")

if df_measures.empty:
    print("❌ Aucune donnée pour la vallée de l'Arve")
    exit(1)

# Filtrer polluants si spécifié
if WANTED_POLLUTANTS:
    df_measures = df_measures[df_measures["Polluant"].isin(WANTED_POLLUTANTS)]
    print(f"Filtrage polluants : {len(df_measures)} lignes restantes")

if df_measures.empty:
    print("❌ Aucune donnée après filtrage des polluants")
    exit(1)

# Lire stations locales (inchangé)
print(f"\n📍 Chargement du fichier stations : {STATIONS_CSV}")
try:
    df_stations = pd.read_csv(STATIONS_CSV, sep=";")
    print(f"Stations chargées : {len(df_stations)} lignes")
except Exception as e:
    print(f"❌ Erreur lecture stations.csv : {e}")
    exit(1)

# Merge mesures + coords
print("\n🔗 Merge des données...")
df_merged = df_measures.merge(
    df_stations,
    left_on="code site",
    right_on="Code",
    how="inner"
)

if df_merged.empty:
    print("❌ Aucune correspondance entre mesures et stations")
    print(f"Codes dans CSV : {df_measures['code site'].unique()}")
    print(f"Codes dans stations.csv : {df_stations['Code'].unique()}")
    exit(1)

# Filtrer les lignes sans coordonnées
df_merged = df_merged[df_merged['Latitude'].notna() & df_merged['Longitude'].notna()]
print(f"✅ Merge réussi : {len(df_merged)} lignes avec coordonnées")

# === SEUILS PAR POLLUANT ===
SEUILS = {
    "NO2": 40,
    "PM10": 50,
    "O3": 100,
}

def couleur_polluant(valeur_max, polluant):
    seuil = SEUILS.get(polluant, 40)
    if valeur_max <= 0.5 * seuil:
        return "#2ecc71"
    elif valeur_max <= seuil:
        return "#f1c40f"
    elif valeur_max <= 1.5 * seuil:
        return "#e67e22"
    else:
        return "#e74c3c"

# --- Grouper par station ---
stations_grouped = df_merged.groupby(['code site', 'nom site', 'Latitude', 'Longitude'])
features = []

for (code_station, nom_station, lat, lon), group in stations_grouped:
    polluants_stats = []
    for polluant, polluant_data in group.groupby('Polluant'):
        valeurs = polluant_data['valeur'].dropna()
        if len(valeurs) > 0:
            polluants_stats.append({
                "polluant": str(polluant),
                "valeur_moyenne": round(float(valeurs.mean()), 2),
                "valeur_max": round(float(valeurs.max()), 2),
                "valeur_min": round(float(valeurs.min()), 2),
                "nb_mesures": int(len(valeurs)),
                "unite": str(polluant_data['unité de mesure'].iloc[0]) if pd.notna(polluant_data['unité de mesure'].iloc[0]) else "µg/m3",
                "date": str(polluant_data['Date de début'].iloc[0])[:10] if pd.notna(polluant_data['Date de début'].iloc[0]) else "N/A"
            })

    if not polluants_stats:
        continue

    couleur = "#2ecc71"
    ordre = {"#2ecc71":1, "#f1c40f":2, "#e67e22":3, "#e74c3c":4}

    for p in polluants_stats:
        couleur_candidate = couleur_polluant(p['valeur_max'], p['polluant'])
        if ordre[couleur_candidate] > ordre.get(couleur, 0):
            couleur = couleur_candidate

    description = f"""<div style="font-family: Arial, sans-serif;">
<h3 style="margin:0 0 10px 0; color:#2c3e50;">{nom_station}</h3>
<p><strong>Code station:</strong> {code_station}</p>
<p><strong>Date:</strong> {polluants_stats[0]['date']}</p>
<table style="width:100%; border-collapse: collapse; font-size:13px;">
<thead>
<tr style="background:#ecf0f1;">
<th style="padding:6px;">Polluant</th>
<th style="padding:6px;">Moyenne</th>
<th style="padding:6px;">Max</th>
<th style="padding:6px;">Min</th>
</tr>
</thead>
<tbody>
"""

    for p in polluants_stats:
        description += f"""
<tr>
<td style="padding:6px;"><strong>{p['polluant']}</strong></td>
<td style="padding:6px; text-align:center;">{p['valeur_moyenne']} {p['unite']}</td>
<td style="padding:6px; text-align:center;">{p['valeur_max']} {p['unite']}</td>
<td style="padding:6px; text-align:center;">{p['valeur_min']} {p['unite']}</td>
</tr>
"""

    description += """
</tbody>
</table>
<p style="font-size:11px; color:#7f8c8d;">Source: ATMO Auvergne-Rhône-Alpes</p>
</div>"""

    features.append({
        "type":"Feature",
        "geometry":{"type":"Point","coordinates":[float(lon), float(lat)]},
        "properties":{
            "name":nom_station,
            "description":description,
            "marker-color":couleur,
            "marker-symbol":"circle"
        }
    })

geojson = {"type":"FeatureCollection","features":features}

with open(OUTPUT_GEOJSON,"w",encoding="utf-8") as f:
    json.dump(geojson,f,ensure_ascii=False, indent=2)

print(f"✅ GeoJSON généré : {OUTPUT_GEOJSON}")
print(f"   {len(features)} stations avec descriptions HTML complètes")






















