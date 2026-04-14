import os
from pathlib import Path

# ============================================================
# CHEMINS SOURCE DES FICHIERS BRUTS
# ============================================================

DATA_DIR        = Path(os.getenv("DATA_DIR", "./data")).resolve()
PARQUET_DIR     = DATA_DIR / "parquet"
METEO_DIR       = DATA_DIR / "meteo"
DUCKDB_PATH     = DATA_DIR / "pestiexpo.duckdb"

# Données administratives
COMMUNE_GPKG    = DATA_DIR / "raw/ADE_4-0_GPKG_WGS84G_FRA-ED2026-02-16.gpkg"
RPG_GPKG          = DATA_DIR / "raw/RPG_3-0__GPKG_LAMB93_FXX_2024-01-01/RPG_Parcelles.gpkg"

# Données de culture et d'IFT
IFT_CSV         = DATA_DIR / "raw/fre-324510908-adonis-ift-2022-v04112024.csv"
CALENDRIER_XLSX    = DATA_DIR / "raw/calendrier_culture_harmonise.xlsx"
CALENDRIER_IDF_CSV = DATA_DIR / "raw/calendrier_idf.csv"
NOMENCLATURE_XLSX = DATA_DIR / "raw/RPG_nomenclatures.xlsx"

# Fichier source des mesures pesticides
MESURES_PESTICIDES_FILE = DATA_DIR / "raw/pesticides_2002_2023_v07_2025.xlsx"

# Activation de la logique météo (optionnelle)
METEO_ENABLED = os.getenv("METEO_ENABLED", "False").strip().lower() in ("1", "true", "yes", "y")

# API météo
METEO_URL_ARCHIVES  = "https://archive-api.open-meteo.com/v1/archive"
METEO_URL_PREVISIONS = "https://api.open-meteo.com/v1/meteofrance"

# ============================================================
# PARAMETRES MÉTÉO
# ============================================================
# Seuils météo pour le phénomène de dispersion
VENT_MAX        = int(19) #Vitesse de vent au delà de laquelle il est interdit de pulvériser"
VENT_DISPERSION_MIN = int(11)
VENT_DISPERSION_MAX = int(5)
PLUIE_SEUIL     = float(2)


# Méthode de discrétisation du risque
METHODE_SEUIL = "valeurs"  #Sinon, "quartiles"
VALEURS_SEUIL = 1,2,4  # Seuils (valeurs des 3 ift cumulé) permettant de créer les classes de risque

# Variables Météo
METEO_CHUNK_SIZE    = int(os.getenv("METEO_CHUNK_SIZE", 100))  # Contrainte liée à l'API de MétéoFrance
DAILY_VARIABLES     = [
    'precipitation_sum',  
    "wind_speed_10m_mean",
]
DAILY_VARIABLES_LOCAL     = [
    "precipitation_sum",
    "wind_speed_10m_mean",
]

HOURLY_VARIABLES    = [
    'precipitation', 
    'wind_speed_10m', 
]


# ============================================================
# TABLE DE CORRESPONDANCE  REGION → CODE REGION RPG
# ============================================================
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

# ============================================================
# TABLE DE CORRESPONDANCE  cultures IFT (ADONIS) → calendrier
# ============================================================
CULTURE_MAPPING: dict[str, str] = {
    # Blé
    "Blé tendre":                                   "Blé tendre",
    "Blé dur":                                      "Blé dur",
    # Orge
    "Orge d'hiver":                                 "Orge d'hiver",
    "Orge de printemps":                            "Orge de printemps",
    # Céréales d'hiver secondaires
    "Triticale":                                    "Céréales d'hiver",
    "Épeautre":                                     "Céréales d'hiver",
    "Seigle d'hiver":                               "Céréales d'hiver",
    # Céréales de printemps / indéterminées
    "Avoine":                                       "Céréales",
    "Seigle de printemps":                          "Céréales",
    "Mélange de céréales":                          "Céréales",
    # Maïs
    "Maïs":                                         "Maïs",
    "Maïs ensilage":                                "Maïs",
    "Maïs doux":                                    "Maïs",
    # Oléagineux
    "Colza":                                        "Colza",
    "Tournesol":                                    "Tournesol",
    "Lin fibres":                                   "Lin printemps",
    "Lin oléagineux":                               "Lin printemps",
    # Cultures diverses
    "Sorgho":                                       "Sorgho",
    "Millet":                                       "Millet",
    "Sarrasin":                                     "Céréales",
    "Riz":                                          "Céréales",
    # Betteraves
    "Betterave non fourragère / Bette":             "Betterave",
    # Pommes de terre
    "Pomme de terre de consommation":               "Pomme de terre de consommation",
    "Pomme de terre féculière":                     "Pomme de terre de consommation",
    "Pomme de terre primeur":                       "Pomme de terre de consommation",
    # Légumineuses
    "Féverole":                                     "Féverole",
    "Pois protéagineux":                            "Pois protéagineux",
    "Petits pois":                                  "Pois de printemps",
    "Lentille cultivée (non fourragère)":           "Pois de printemps",
    "Pois chiche":                                  "Pois de printemps",
    "Mélange de légumineuses":                      "Pois protéagineux",
    "Mélange de légumineuses fourragères":          "Luzerne",
    "Mélange de légumineuses fourragères (entre elles)": "Luzerne",
    "Mélange de protéagineux":                      "Pois protéagineux",
    "Autre mélange de plantes fixant l'azote":      "Luzerne",
    # Soja → légumineuse la plus proche agronomiquement
    "Soja":                                         "Féverole d'hiver",
    # Lin
    "Lin d'hiver":                                  "Lin d'hiver",
    # Luzerne
    "Luzerne déshydratée":                          "Luzerne",
    "Autre luzerne":                                "Luzerne",
    "Autre sainfoin":                               "Luzerne",
    "Autre trèfle":                                 "Luzerne",
    # Prairies et surfaces enherbées
    "Prairie permanente":                           "Prairies",
    "Prairie en rotation longue (6 ans ou plus)":  "Prairies",
    "Autre prairie temporaire de 5 ans ou moins":  "Prairies",
    "Ray-grass de 5 ans ou moins":                 "Prairies",
    "Surface pastorale (SPH)":                     "Prairies",
    "Surface pastorale (SPL)":                     "Prairies",
    "Bois pâturé":                                  "Prairies",
    # Vigne
    "Vigne":                                        "Vigne",
    "Vigne : raisins de cuve":                      "Vigne",
    # Arboriculture
    "Vergers":                                      "Arboriculture",
    "Cerise bigarreau pour transformation":         "Arboriculture",
    "Prune d'Ente pour transformation":             "Arboriculture",
    "Noisette":                                     "Arboriculture",
    "Noix":                                         "Arboriculture",
    "Châtaigne":                                    "Arboriculture",
    "Châtaigneraie":                                "Arboriculture",
    "Agrume":                                       "Arboriculture",
    "Oliveraie":                                    "Arboriculture",
    "Petit fruit rouge":                            "Arboriculture",
    "Fraise":                                       "Arboriculture",
}
