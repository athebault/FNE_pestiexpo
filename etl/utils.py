"""
Utilitaires partagés — ETL PestiExpo

Contient :
  - fetch_meteo_chunk / fetch_all_communes  : appels API Open-Meteo (archives + prévisions)
  - normaliser_colonnes_meteo               : normalise les noms de colonnes ERA5 local → standard
  - check_docker_era5                       : vérifie que le conteneur ERA5 local répond
  - get_duckdb / write_to_duckdb_table      : helpers DuckDB (connexion + écriture)
  - load_communes                           : charge et filtre le référentiel communes
"""

import requests
import pandas as pd
import polars as pl
import duckdb
import time
import logging

from etl_config import METEO_CHUNK_SIZE, DUCKDB_PATH, PARQUET_DIR, code_region_rpg

logger = logging.getLogger(__name__)


# ============================================================
# API Open-Meteo — requêtes HTTP
# ============================================================

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


# ============================================================
# ERA5 local (Docker)
# ============================================================

LOCAL_API = "http://127.0.0.1:8080/v1/archive"


def check_docker_era5() -> bool:
    """Vérifie si le conteneur Open-Meteo ERA5 local répond."""
    try:
        requests.get(LOCAL_API.replace("/v1/archive", "/"), timeout=5)
        return True
    except requests.ConnectionError:
        return False


# ============================================================
# Normalisation des colonnes météo
# ============================================================

# Colonnes communes aux 3 sources (après normalisation)
METEO_COLUMNS = [
    "code_insee", "time",
    "temperature_2m_max", "temperature_2m_min",
    "precipitation_sum", "wind_speed_10m_mean",
]


def normaliser_colonnes_meteo(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalise les noms de colonnes pour les rendre identiques entre les 3 sources météo :
      - date               → time                (ERA5 local utilise "date")
      - wind_gusts_10m_max → wind_speed_10m_mean (ERA5 local utilise les rafales comme proxy du vent moyen)
    Puis sélectionne uniquement les colonnes standard disponibles.
    """
    renames = {}
    if "date" in df.columns:
        renames["date"] = "time"
    if "wind_gusts_10m_max" in df.columns:
        if "wind_speed_10m_mean" in df.columns:
            # wind_speed_10m_mean déjà présent : supprimer les rafales (inutiles)
            df = df.drop("wind_gusts_10m_max")
        else:
            renames["wind_gusts_10m_max"] = "wind_speed_10m_mean"
    if renames:
        df = df.rename(renames)
    available = [c for c in METEO_COLUMNS if c in df.columns]
    return df.select(available)


# ============================================================
# DuckDB
# ============================================================

def get_duckdb(read_only: bool = False) -> duckdb.DuckDBPyConnection:
    """Ouvre une connexion DuckDB sur la base PestiExpo."""
    return duckdb.connect(str(DUCKDB_PATH), read_only=read_only)


def write_to_duckdb_table(
    df: pl.DataFrame,
    table: str,
    create_sql: str,
    delete_where: str | None = None,
) -> None:
    """
    Écrit un DataFrame Polars dans une table DuckDB.

    Args:
        df:           Données à écrire
        table:        Nom de la table cible
        create_sql:   Instruction CREATE TABLE IF NOT EXISTS complète
        delete_where: Clause WHERE pour supprimer les lignes existantes avant insertion
                      (ex: "year(date) = 2026 AND insee_com IN ('44001', '44002')")
                      Si None, la table est vidée entièrement (DROP + CREATE).
    """
    con = get_duckdb()
    if delete_where is None:
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(create_sql)
    else:
        con.execute(create_sql)
        con.execute(f"DELETE FROM {table} WHERE {delete_where}")
    con.execute(f"INSERT INTO {table} SELECT * FROM df")
    con.close()


# ============================================================
# Référentiel communes
# ============================================================

def load_communes(region: str | None = None) -> pl.DataFrame:
    """
    Charge le référentiel communes depuis le parquet, avec filtre région optionnel.

    Args:
        region: Nom de région (ex: "Pays de la Loire"). None = toutes régions.

    Returns:
        DataFrame avec colonnes code_insee, latitude, longitude, code_insee_reg, …
    """
    communes = pl.read_parquet(PARQUET_DIR / "communes_admin.parquet")

    if region is not None:
        if region not in code_region_rpg:
            valides = ", ".join(sorted(code_region_rpg.keys()))
            raise ValueError(f"Région inconnue : '{region}'. Valides : {valides}")
        code_reg = code_region_rpg[region]
        communes = communes.filter(pl.col("code_insee_reg") == code_reg)
        logger.info(f"  Région : {region} (code {code_reg}) — {communes.shape[0]} communes")
    else:
        logger.info(f"  {communes.shape[0]} communes chargées (toutes régions)")

    if communes.is_empty():
        raise ValueError(f"Aucune commune trouvée pour la région '{region}'.")

    return communes
