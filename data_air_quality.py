def download_csv(url):
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
        # Vérifier si c'est bien un CSV et pas du HTML
        if 'text/csv' in r.headers.get('Content-Type', '') or len(r.text) > 100:
            print("=== APERÇU (200 premiers caractères) ===")
            print(r.text[:200])
            print("=========================================")
            
            try:
                df = pd.read_csv(io.StringIO(r.text), sep=";")
                if df.empty:
                    print("⚠️ Vide avec sep=';', test avec ','")
                    df = pd.read_csv(io.StringIO(r.text), sep=",")
                
                print(f"✅ CSV parsé : {len(df)} lignes")
                return df
            except Exception as e:
                print(f"❌ Erreur : {e}")
                return pd.DataFrame()
    
    return pd.DataFrame()












