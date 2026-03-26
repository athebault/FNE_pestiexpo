"""
Utilitaires partagés - PestiExpo
Fonctions communes d'accès à l'API Open-Meteo.
"""

import requests
import pandas as pd
import polars as pl
import time
import logging

from config import METEO_CHUNK_SIZE

logger = logging.getLogger(__name__)


def fetch_meteo_chunk(url: str, chunk: pd.DataFrame, extra_params: dict) -> list[dict]:
    """
    Requête vers l'API météo pour un chunk de communes, avec retry sur 429.

    Args:
        url:          URL de l'API (archives ou prévisions)
        chunk:        DataFrame pandas avec colonnes latitude, longitude, code_insee
        extra_params: Paramètres spécifiques à la requête (dates, variables, forecast_days…)

    Returns:
        Liste de résultats JSON (un par commune du chunk)
    """
    params = {
        "latitude":  ",".join(chunk["latitude"].round(4).astype(str)),
        "longitude": ",".join(chunk["longitude"].round(4).astype(str)),
        **extra_params,
    }

    for tentative in range(5):
        r = requests.get(url, params=params, timeout=60)

        if r.status_code == 429:
            wait = 60 * (tentative + 1)
            logger.warning(f"Rate limit, attente {wait}s...")
            time.sleep(wait)
            continue

        if r.status_code != 200:
            logger.error(f"HTTP {r.status_code} : {r.text[:500]}")
            logger.debug(f"URL envoyée : {r.url[:500]}")

        r.raise_for_status()
        result = r.json()
        return result if isinstance(result, list) else [result]

    raise RuntimeError("Rate limit : max tentatives atteintes.")


def fetch_all_communes(
    communes: pl.DataFrame,
    url: str,
    extra_params: dict,
    label: str = "météo",
) -> pl.DataFrame:
    """
    Récupère les données météo pour toutes les communes en chunks séquentiels.
    communes doit avoir les colonnes : code_insee, latitude, longitude.

    Args:
        communes:     DataFrame Polars des communes
        url:          URL de l'API
        extra_params: Paramètres communs à tous les chunks (dates, variables…)
        label:        Libellé pour les logs (ex: "météo", "prévisions")

    Returns:
        DataFrame Polars avec colonnes daily + code_insee, colonne time en Date
    """
    communes_pd = (
        communes.select(["code_insee", "latitude", "longitude"])
        .to_pandas()
        .reset_index(drop=True)
    )
    chunks = [
        communes_pd.iloc[i:i + METEO_CHUNK_SIZE].reset_index(drop=True)
        for i in range(0, len(communes_pd), METEO_CHUNK_SIZE)
    ]
    logger.info(f"Récupération {label} : {len(communes_pd)} communes en {len(chunks)} chunks")

    dfs = []
    for i, chunk in enumerate(chunks):
        logger.info(f"  Chunk {i+1}/{len(chunks)}...")
        try:
            results = fetch_meteo_chunk(url, chunk, extra_params)
            for res, (_, row) in zip(results, chunk.iterrows()):
                if "daily" not in res:
                    logger.warning(f"Pas de données pour {row['code_insee']}")
                    continue
                df = pd.DataFrame(res["daily"])
                df["code_insee"] = row["code_insee"]
                dfs.append(df)
            time.sleep(0.5)
        except requests.exceptions.HTTPError as e:
            logger.error(f"Erreur chunk {i+1} : {e}", exc_info=True)
        except Exception as e:
            logger.error(f"Erreur chunk {i+1} : {e}", exc_info=True)

    if not dfs:
        raise ValueError(f"Aucune donnée {label} récupérée.")

    meteo = pl.from_pandas(pd.concat(dfs, ignore_index=True))
    meteo = meteo.with_columns(pl.col("time").str.to_date())
    return meteo
