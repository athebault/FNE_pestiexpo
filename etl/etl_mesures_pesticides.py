"""
ETL Mesures Pesticides - PestiExpo
Charge les mesures + récupère la météo ciblée (communes × périodes concernées)
Paramètres météo : température, vent, pluviométrie, humidité relative, ensoleillement

Lancement:
    uv run python3 etl/etl_mesures_pesticides.py             # utilise le cache météo si présent
    uv run python3 etl/etl_mesures_pesticides.py --force     # re-télécharge la météo
"""

import polars as pl
import pandas as pd
import logging
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyproj import Transformer

from etl.etl_config import PARQUET_DIR, METEO_DIR, METEO_URL_ARCHIVES, METEO_CHUNK_SIZE, MESURES_PESTICIDES_FILE, METEO_ENABLED
from utils import fetch_meteo_chunk

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Paramètres météo spécifiques à cette analyse
DAILY_VARIABLES_MESURES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "temperature_2m_mean",
    "precipitation_sum",
    "wind_speed_10m_max",
    "wind_speed_10m_mean",        # vent moyen (pas seulement max)
    "relative_humidity_2m_max",   # humidité relative
    "relative_humidity_2m_min",
    "relative_humidity_2m_mean",
    "solar_radiation",          
    "et0_fao_evapotranspiration",
]

MARGE_JOURS  = 15   # jours avant/après le prélèvement
MAX_WORKERS  = 5    # requêtes parallèles vers l'API météo
METEO_BRUT_PATH = METEO_DIR / "mesures/meteo_mesures_brut.parquet"


# ============================================================
# 1. CHARGEMENT DES MESURES
# ============================================================
def load_mesures(fichier: Path) -> pl.DataFrame:
    logger.info(f"Chargement mesures : {fichier}")

    df = pl.read_excel(fichier, sheet_name="pesticides_2002_2023_v-07-2025")

    rename_map = {
        "AASQA":                 "reseau",
        "Commune":               "nom_commune",
        "Code INSEE":            "code_insee",
        "xlamb93":               "x_lamb93",
        "ylamb93":               "y_lamb93",
        "Debut prelevement":     "debut_prelevement",
        "Fin prelevement":       "fin_prelevement",
        "Annee":                 "annee",
        "Semaine":               "semaine",
        "jours de prelevement":  "nb_jours_prelevement",
        "Coupure PM":            "type_filtre",
        "prelevement":           "nb_prelevement",
        "substance active":      "substance",
        "Concentration (ng/m3)": "concentration_ng_m3"
    }
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(rename_map)

    # Conversion Lambert 93 → WGS84
    transformer = Transformer.from_crs("EPSG:2154", "EPSG:4326", always_xy=True)
    x = df["x_lamb93"].cast(pl.Float64).to_numpy()
    y = df["y_lamb93"].cast(pl.Float64).to_numpy()
    lon, lat = transformer.transform(x, y)

    df = df.with_columns([
        pl.col("code_insee").cast(pl.Utf8).str.zfill(5),
        pl.col("code_insee").cast(pl.Utf8).str.zfill(5).str.slice(0, 2).alias("code_insee_dep"),
        pl.col("debut_prelevement").cast(pl.Datetime).dt.date(),
        pl.col("fin_prelevement").cast(pl.Datetime).dt.date(),
        pl.when(pl.col("concentration_ng_m3").cast(pl.Utf8).str.contains("<"))
          .then(0.0)
          .otherwise(pl.col("concentration_ng_m3").cast(pl.Float64))
          .alias("concentration_ng_m3"),
        pl.when(pl.col("concentration_ng_m3").cast(pl.Utf8).str.contains("<"))
          .then(False).otherwise(True)
          .alias("detecte"),
        pl.Series("longitude", lon),
        pl.Series("latitude", lat),
    ])

    logger.info(f"  → {df.shape[0]} mesures | {df['code_insee'].n_unique()} communes | {df['annee'].min()}→{df['annee'].max()}")
    return df


# ============================================================
# 2. RÉCUPÉRATION MÉTÉO CIBLÉE (parallèle)
# ============================================================
def _fetch_one_chunk(extra_params: dict, start: str, end: str, chunk: pd.DataFrame) -> list[pd.DataFrame]:
    """Requête pour un chunk (délègue à utils). Retourne une liste de DataFrames."""
    try:
        results = fetch_meteo_chunk(METEO_URL_ARCHIVES, chunk, extra_params)
        rows = []
        for res, (_, row) in zip(results, chunk.iterrows()):
            if "daily" not in res:
                continue
            df = pd.DataFrame(res["daily"])
            df["code_insee"] = row["code_insee"]
            df["start_date"] = start
            df["end_date"]   = end
            rows.append(df)
        return rows
    except Exception as e:
        logger.error(f"Erreur chunk : {e}")
        return []


def fetch_meteo_ciblee(mesures: pl.DataFrame) -> pl.DataFrame:
    """
    Récupère la météo uniquement pour les stations et périodes des mesures.
    Utilise les coordonnées WGS84 converties depuis Lambert 93.
    Les requêtes sont exécutées en parallèle (MAX_WORKERS).
    """
    sans_coords = mesures.filter(pl.col("latitude").is_null())
    if sans_coords.shape[0] > 0:
        logger.warning(f"  ⚠ {sans_coords['code_insee'].n_unique()} communes sans coordonnées")

    # Dédupliquer stations × périodes étendues
    requetes = (
        mesures
        .filter(pl.col("latitude").is_not_null())
        .select(["code_insee", "latitude", "longitude", "debut_prelevement", "fin_prelevement"])
        .unique()
        .with_columns([
            (pl.col("debut_prelevement") - pl.duration(days=MARGE_JOURS))
              .dt.strftime("%Y-%m-%d").alias("start_date"),
            (pl.col("fin_prelevement") + pl.duration(days=MARGE_JOURS))
              .dt.strftime("%Y-%m-%d").alias("end_date"),
        ])
    )

    periodes = (
        requetes.select(["start_date", "end_date"]).unique().sort(["start_date", "end_date"])
    )
    logger.info(f"  {periodes.shape[0]} périodes × {requetes['code_insee'].n_unique()} communes")

    # Construire toutes les tâches (params + métadonnées) en avance
    tasks = []
    for start, end in periodes.iter_rows():
        communes_periode = (
            requetes
            .filter((pl.col("start_date") == start) & (pl.col("end_date") == end))
            .select(["code_insee", "latitude", "longitude"])
            .unique()
            .to_pandas().reset_index(drop=True)
        )
        extra_params = {
            "start_date": start,
            "end_date":   end,
            "daily":      DAILY_VARIABLES_MESURES,
            "timezone":   "Europe/Paris",
        }
        for j in range(0, len(communes_periode), METEO_CHUNK_SIZE):
            chunk = communes_periode.iloc[j:j + METEO_CHUNK_SIZE].reset_index(drop=True)
            tasks.append((extra_params, start, end, chunk))

    logger.info(f"  {len(tasks)} requêtes API à exécuter ({MAX_WORKERS} en parallèle)")

    all_meteo = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_one_chunk, p, s, e, c): i
            for i, (p, s, e, c) in enumerate(tasks)
        }
        for i, future in enumerate(as_completed(futures), 1):
            rows = future.result()
            all_meteo.extend(rows)
            if i % 50 == 0 or i == len(tasks):
                logger.info(f"  {i}/{len(tasks)} requêtes traitées")

    if not all_meteo:
        raise ValueError("Aucune donnée météo récupérée.")

    meteo = pl.from_pandas(pd.concat(all_meteo, ignore_index=True))
    meteo = meteo.with_columns(pl.col("time").str.to_date())
    return meteo


# ============================================================
# 3. AGRÉGATION MÉTÉO PAR PÉRIODE DE PRÉLÈVEMENT
# ============================================================
def aggregate_meteo_par_prelevement(
    mesures: pl.DataFrame,
    meteo: pl.DataFrame
) -> pl.DataFrame:
    """
    Pour chaque mesure, agrège la météo sur la fenêtre étendue (±15j).
    """
    meteo_agg = (
        meteo
        .group_by(["code_insee", "start_date", "end_date"])
        .agg([
            # Température
            pl.col("temperature_2m_max").mean().alias("temp_max_moy"),
            pl.col("temperature_2m_min").mean().alias("temp_min_moy"),
            pl.col("temperature_2m_mean").mean().alias("temp_moy"),
            # Vent
            pl.col("wind_speed_10m_max").mean().alias("vent_max_moy"),
            pl.col("wind_speed_10m_mean").mean().alias("vent_moy"),
            pl.col("wind_speed_10m_max").max().alias("vent_max_abs"),
            # Précipitations
            pl.col("precipitation_sum").sum().alias("precip_totale"),
            pl.col("precipitation_sum").mean().alias("precip_moy_jour"),
            # Humidité
            pl.col("relative_humidity_2m_mean").mean().alias("humidite_moy"),
            pl.col("relative_humidity_2m_max").mean().alias("humidite_max_moy"),
            # Ensoleillement (secondes → heures)
            (pl.col("sunshine_duration").sum() / 3600).alias("ensoleillement_h_total"),
            (pl.col("sunshine_duration").mean() / 3600).alias("ensoleillement_h_moy"),
            # Indicateurs risque dispersion
            (
                (pl.col("wind_speed_10m_max") >= 4) &
                (pl.col("wind_speed_10m_max") < 11)
            ).sum().alias("nb_jours_dispersion"),
            (pl.col("wind_speed_10m_max") >= 11).sum().alias("nb_jours_interdiction"),
            (pl.col("precipitation_sum") > 0).sum().alias("nb_jours_pluie"),
        ])
        .with_columns([
            pl.col("start_date").cast(pl.Utf8).str.to_date(),
            pl.col("end_date").cast(pl.Utf8).str.to_date(),
        ])
    )

    enrichi = mesures.with_columns([
        (pl.col("debut_prelevement") - pl.duration(days=MARGE_JOURS)).alias("start_date"),
        (pl.col("fin_prelevement")   + pl.duration(days=MARGE_JOURS)).alias("end_date"),
    ]).join(meteo_agg, on=["code_insee", "start_date", "end_date"], how="left")

    nb_enrichies = enrichi.filter(pl.col("vent_moy").is_not_null()).shape[0]
    logger.info(f"  → {nb_enrichies}/{enrichi.shape[0]} mesures enrichies avec météo")
    return enrichi


# ============================================================
# 4. PIPELINE PRINCIPAL
# ============================================================
def run(force: bool = False):
    # 1. Charger mesures
    mesures = load_mesures(MESURES_PESTICIDES_FILE)

    if not METEO_ENABLED:
        logger.info("METEO_ENABLED=False : ETL mesures pesticides (météo) ignoré")
        out = PARQUET_DIR / "mesures_pesticides_meteo.parquet"
        PARQUET_DIR.mkdir(parents=True, exist_ok=True)
        mesures.write_parquet(out)
        logger.info(f"✓ mesures_pesticides_meteo.parquet créé en mode sans météo : {out}")
        return mesures

    # 2. Météo ciblée — cache si déjà présent
    if not force and METEO_BRUT_PATH.exists():
        logger.info(f"Cache météo trouvé, chargement sans re-fetch : {METEO_BRUT_PATH}")
        logger.info("  → Utilisez --force pour re-télécharger")
        meteo = pl.read_parquet(METEO_BRUT_PATH)
        logger.info(f"  {meteo.shape[0]} lignes chargées")
    else:
        logger.info("Récupération météo ciblée...")
        meteo = fetch_meteo_ciblee(mesures)
        METEO_BRUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        meteo.write_parquet(METEO_BRUT_PATH)
        logger.info(f"✓ Cache sauvegardé : {METEO_BRUT_PATH} ({meteo.shape[0]} lignes)")

    # 4. Agrégation et jointure
    enrichi = aggregate_meteo_par_prelevement(mesures, meteo)
    enrichi.write_parquet(PARQUET_DIR / "mesures_pesticides_meteo.parquet")
    logger.info("✓ mesures_pesticides_meteo.parquet")

    print(enrichi.select([
        "code_insee", "nom_commune", "substance", "annee", "semaine",
        "concentration_ng_m3", "detecte",
        "vent_moy", "temp_moy", "humidite_moy", "ensoleillement_h_moy",
        "precip_totale", "nb_jours_dispersion"
    ]).head(10))

    return enrichi


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true",
                        help="Re-télécharger la météo même si le cache existe")
    args = parser.parse_args()
    run(force=args.force)
