import os
from pathlib import Path

DATA_DIR        = Path(os.getenv("DATA_DIR", "./data"))
PARQUET_DIR     = DATA_DIR / "parquet"
METEO_DIR       = DATA_DIR / "meteo"
DUCKDB_PATH     = DATA_DIR / "pestiexpo.duckdb"

# Seuils météo pour le phénomène de dispersion
VENT_MAX        = int(os.getenv("VENT_MAX", 11))
VENT_DISPERSION = int(os.getenv("VENT_DISPERSION", 4))
PLUIE_SEUIL     = float(os.getenv("PLUIE_SEUIL", 0))

# API météo
METEO_URL_ARCHIVES  = "https://archive-api.open-meteo.com/v1/archive"
METEO_URL_PREVISIONS = "https://api.open-meteo.com/v1/meteofrance"
METEO_CHUNK_SIZE    = int(os.getenv("METEO_CHUNK_SIZE", 100))  # Contrainte liée à l'API de MétéoFrance
DAILY_VARIABLES     = [
    "temperature_2m_max",
    "temperature_2m_min", 
    "precipitation_sum",
    "wind_speed_10m_max",
    "et0_fao_evapotranspiration"
]
HOURLY_VARIABLES    = [
    "temperature_2m", "precipitation",
    "wind_speed_10m", "et0_fao_evapotranspiration"
]

# Codes région pour récupération des données parcelllaires RPG
code_region_rpg = {
   "Guadeloupe": "01", 
   "Martinique":"02", 
   "Guyane": "03", 
   "La Réunion":"04", 
   "Mayotte": "06",
   "Île-de-France": "11",
   "Centre-Val de Loire": "24",
   "Bourgogne-Franche-Comté": "27",
   "Normandie": "28",
   "Hauts-de-France": "32",
   "Grand Est": "44",
   "Pays de la Loire": "52",
   "Bretagne":"53",
   "Nouvelle-Aquitaine": "75",
   "Occitanie":"76",
   "Auvergne-Rhône-Alpes": "84",
   "Provence-Alpes-Côte d'Azur":"93",
   "Corse":"94"
}