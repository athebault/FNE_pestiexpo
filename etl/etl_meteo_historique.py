"""
ETL Météo Historique - PestiExpo
Récupère les archives météo pour toutes les communes
et calcule les indicateurs de risque pesticides.

Lancement: 
    # Indicateurs uniquement (léger)
    uv run python3 etl/etl_meteo_historique.py --annee 2026

    # Indicateurs + données brutes journalières
    uv run python3 etl/etl_meteo_historique.py --annee 2026 --save_brut
"""

import requests
import pandas as pd
import polars as pl
import numpy as np
import logging
import time

from pathlib import Path
from datetime import datetime

from config import (
    PARQUET_DIR, METEO_DIR,
    VENT_MAX, VENT_DISPERSION, PLUIE_SEUIL,
    METEO_URL_ARCHIVES, METEO_CHUNK_SIZE, DAILY_VARIABLES
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ============================================================
# 1. RECUPERATION METEO (batch par chunks obligatoire pour API meteo)
# ============================================================
def fetch_meteo_chunk(chunk: pd.DataFrame, start_date: str, end_date: str) -> list[dict]:
    """Récupère la météo pour un chunk de communes via l'API Open-Meteo."""
    params = {
        "latitude":   ",".join(chunk["latitude"].round(4).astype(str)),
        "longitude":  ",".join(chunk["longitude"].round(4).astype(str)),
        "start_date": start_date,
        "end_date":   end_date,
        "daily":      DAILY_VARIABLES,
        "timezone":   "Europe/Paris",
    }

    r = requests.get(METEO_URL_ARCHIVES, params=params, timeout=60)
    # Log de la réponse:
    if r.status_code != 200:
        logger.error(f"HTTP {r.status_code} : {r.text[:500]}")
        r.raise_for_status()

    return r.json()


import time

def fetch_meteo_all_communes(communes: pl.DataFrame, start_date: str, end_date: str) -> pl.DataFrame:

    communes_pd = communes.select(["code_insee", "latitude", "longitude"]).to_pandas().reset_index(drop=True)
    chunks = [
        communes_pd.iloc[i:i + METEO_CHUNK_SIZE].reset_index(drop=True)
        for i in range(0, len(communes_pd), METEO_CHUNK_SIZE)
    ]

    logger.info(f"Récupération météo : {len(communes_pd)} communes en {len(chunks)} chunks")

    dfs = []
    for i, chunk in enumerate(chunks):
        logger.info(f"  Chunk {i+1}/{len(chunks)}...")

        # Retry avec backoff en cas de 429
        for tentative in range(5):
            try:
                results = fetch_meteo_chunk(chunk, start_date, end_date)
                if isinstance(results, dict):
                    results = [results]

                for j, (res, (_, row)) in enumerate(zip(results, chunk.iterrows())):
                    if "daily" not in res:
                        logger.warning(f"Pas de données pour {row['code_insee']}")
                        continue
                    df = pd.DataFrame(res["daily"])
                    df["code_insee"] = row["code_insee"]
                    dfs.append(df)

                time.sleep(0.5)  # délai entre chaque chunk
                break  # succès, on sort du retry

            except Exception as e:
                if "429" in str(e):
                    wait = 60 * (tentative + 1)
                    logger.warning(f"Rate limit atteint, attente {wait}s...")
                    time.sleep(wait)
                else:
                    logger.error(f"Erreur chunk {i+1} : {e}", exc_info=True)
                    break

    if not dfs:
        raise ValueError("Aucune donnée météo récupérée.")

    meteo = pl.from_pandas(pd.concat(dfs, ignore_index=True))
    meteo = meteo.with_columns(pl.col("time").str.to_date())
    return meteo

# ============================================================
# 2. CALCUL DES INDICATEURS
# ============================================================
def compute_indicateurs(meteo: pl.DataFrame) -> pl.DataFrame:
    """Calcule les indicateurs de risque à partir des données météo brutes."""

    meteo = meteo.with_columns([
        # Interdiction de pulvérisation (vent trop fort)
        (pl.col("wind_speed_10m_max") >= VENT_MAX).alias("interdiction_pulv"),

        # Pluie limitant la dispersion
        (pl.col("precipitation_sum") >= PLUIE_SEUIL).alias("pluie_limitant_dispersion"),

        # Risque de dispersion (vent dans la zone à risque)
        (
            (pl.col("wind_speed_10m_max") >= VENT_DISPERSION) &
            (pl.col("wind_speed_10m_max") <  VENT_MAX)
        ).alias("risque_dispersion"),
    ])

    return meteo


def aggregate_indicateurs(meteo: pl.DataFrame) -> pl.DataFrame:
    """Agrège les indicateurs par commune sur toute la période."""

    return (
        meteo.group_by("code_insee")
        .agg([
            # Indicateurs de risque (nb jours)
            pl.col("interdiction_pulv").sum().alias("nb_jours_interdiction"),
            pl.col("pluie_limitant_dispersion").sum().alias("nb_jours_pluie_limitante"),
            pl.col("risque_dispersion").sum().alias("nb_jours_dispersion"),

            # Statistiques météo brutes
            pl.col("wind_speed_10m_max").mean().alias("vent_moyen"),
            pl.col("wind_speed_10m_max").max().alias("vent_max_obs"),
            pl.col("precipitation_sum").sum().alias("precip_totale"),
            pl.col("temperature_2m_max").mean().alias("temp_max_moyenne"),
            pl.col("temperature_2m_min").mean().alias("temp_min_moyenne"),
            pl.col("et0_fao_evapotranspiration").sum().alias("eto_total"),

            # Nb jours observés
            pl.len().alias("nb_jours_obs"),
        ])
        .sort("code_insee")
    )


# ============================================================
# 3. SAUVEGARDE
# ============================================================
def save_meteo(meteo: pl.DataFrame, annee: int):
    """Sauvegarde les données météo brutes partitionnées par année."""
    path = METEO_DIR / f"historique/annee={annee}"
    path.mkdir(parents=True, exist_ok=True)
    meteo.write_parquet(path / "meteo.parquet")
    logger.info(f"✓ Météo brute sauvegardée : {path}")


def save_indicateurs(indicateurs: pl.DataFrame, annee: int):
    """Sauvegarde les indicateurs agrégés."""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    path = PARQUET_DIR / f"indicateurs_meteo_{annee}.parquet"
    indicateurs.write_parquet(path)
    logger.info(f"✓ Indicateurs sauvegardés : {path} ({indicateurs.shape[0]} communes)")


# ============================================================
# 4. PIPELINE PRINCIPAL
# ============================================================
def run(annee: int = 2026, save_brut: bool = False):
    """
    Pipeline complet :
    1. Charger les communes
    2. Récupérer la météo
    3. Calculer les indicateurs
    4. Sauvegarder
    """
    aujourd_hui = datetime.today()
    start_date  = f"{annee}-01-01"
    end_date    = aujourd_hui.strftime("%Y-%m-%d") if annee == aujourd_hui.year else f"{annee}-12-31"

    logger.info(f"ETL Météo Historique — {annee} ({start_date} → {end_date})")

    # 1. Communes
    communes = pl.read_parquet(PARQUET_DIR / "communes.parquet")
    logger.info(f"  {communes.shape[0]} communes chargées")

    # 2. Météo brute
    meteo = fetch_meteo_all_communes(communes, start_date, end_date)
    logger.info(f"  {meteo.shape[0]} lignes météo récupérées")

    # 3. Indicateurs
    meteo_avec_indicateurs = compute_indicateurs(meteo)
    indicateurs = aggregate_indicateurs(meteo_avec_indicateurs)

    # 4. Sauvegarde
    if save_brut:
        save_meteo(meteo_avec_indicateurs, annee)
    save_indicateurs(indicateurs, annee)

    logger.info("ETL Météo Historique terminé ✓")
    return indicateurs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--annee",     type=int,  default=datetime.today().year)
    parser.add_argument("--save_brut", action="store_true",
                        help="Sauvegarder aussi les données météo brutes journalières")
    args = parser.parse_args()
    run(annee=args.annee, save_brut=args.save_brut)