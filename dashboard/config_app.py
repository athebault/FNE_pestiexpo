"""
Configuration du dashboard PestiExpo.
Constantes visuelles, chemins, et libellés.
"""

import sys
from pathlib import Path

# ── Chemins ──────────────────────────────────────────────────
ROOT       = Path(__file__).resolve().parent.parent
DB_PATH    = ROOT / "data" / "pestiexpo.duckdb"
PARQUET    = ROOT / "data" / "parquet"
GEOJSON_PATH = PARQUET / "communes_geo.geojson"

# Accès au module ETL (CULTURE_MAPPING, etc.)
sys.path.insert(0, str(ROOT / "etl"))

# ── Palette & états ──────────────────────────────────────────
#   Différence explicite entre "pas de données calendrier" et "pas de traitement ce jour"
STATES: dict[str | int, tuple[str, str]] = {
    "no_calendar": ("#78909C", "Hors calendrier (culture non couverte)"),
    "no_data":     ("#CFD8DC", "Données de risque non disponibles"),
    0:             ("#A5D6A7", "Aucun traitement ce jour"),
    1:             ("#FFF176", "Risque faible"),
    2:             ("#FFB300", "Risque modéré"),
    3:             ("#F57C00", "Risque élevé"),
    4:             ("#B71C1C", "Risque très élevé"),
}

STATE_ORDER = ["no_calendar", "no_data", 0, 1, 2, 3, 4]

REG_NOMS = {
    "01": "Guadeloupe", "02": "Martinique", "03": "Guyane", "04": "La Réunion",
    "06": "Mayotte", "11": "Île-de-France", "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté", "28": "Normandie", "32": "Hauts-de-France",
    "44": "Grand Est", "52": "Pays de la Loire", "53": "Bretagne",
    "75": "Nouvelle-Aquitaine", "76": "Occitanie", "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur", "94": "Corse",
}
