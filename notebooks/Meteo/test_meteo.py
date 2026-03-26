# Test rapide à lancer en dehors de la fonction
import requests
import pandas as pd
from config import PARQUET_DIR, METEO_URL_ARCHIVES, DAILY_VARIABLES

communes = pd.read_parquet(PARQUET_DIR / "communes.parquet")[["code_insee", "latitude", "longitude"]].head(5)

params = {
    "latitude":   ",".join(communes["latitude"].astype(str)),
    "longitude":  ",".join(communes["longitude"].astype(str)),
    "start_date": "2026-01-01",
    "end_date":   "2026-03-25",
    "daily":      DAILY_VARIABLES,
    "timezone":   "Europe/Paris",
}

r = requests.get(METEO_URL_ARCHIVES, params=params, timeout=60)
print(f"Status : {r.status_code}")
print(r.json())