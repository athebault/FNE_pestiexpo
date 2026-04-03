"""
ETL Météo — PestiExpo
Pipeline en 3 couches pour récupérer les données météo :

  1. ERA5 local (Docker)    : données historiques (1500 jours: ~ 4ans), sans limite de requêtes
  2. Archive distante       : comble le gap entre la dernière date ERA5 et hier
  3. Prévisions (J+7)       : récupérées depuis l'API MétéoFrance / Open-Meteo

Ce script ne calcule PAS les indicateurs de risque — c'est le rôle de calcul_risque_journalier.py.
Il sauvegarde uniquement les données brutes normalisées.

Sorties :
  data/meteo/historique/{annee}/meteo.parquet - données brutes historiques(ERA5 + gap)
  data/meteo/previsions/meteo_previsions.parquet - prévisions brutes

Usage :
  uv run python3 etl/etl_meteo.py --annee 2026
  uv run python3 etl/etl_meteo.py --annee 2026 --region "Pays de la Loire"
  uv run python3 etl/etl_meteo.py --distant          # sans Docker ERA5
  uv run python3 etl/etl_meteo.py --test             # 10 communes (mode local uniquement)

Prérequis mode local (Docker ERA5) :
  docker run -d --rm -v open-meteo-data:/app/data -p 8080:8080 ghcr.io/open-meteo/open-meteo
"""

import polars as pl
import requests
import logging
import time

from datetime import date, timedelta

from config import (
    METEO_DIR,
    METEO_URL_ARCHIVES, METEO_URL_PREVISIONS,
    DAILY_VARIABLES, DAILY_VARIABLES_LOCAL,
)
from utils import (
    fetch_all_communes, load_communes,
    check_docker_era5, normaliser_colonnes_meteo,
    LOCAL_API, METEO_COLUMNS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ERA5_MODEL = "era5"
ERA5_BATCH = 50   # communes par requête POST


# ============================================================
# Source 1 : ERA5 local (Docker)
# ============================================================
def _fetch_era5_batch(communes: list[dict], start_date: str, end_date: str) -> list[dict]:
    """Requête POST sur l'API ERA5 locale pour un batch de communes."""
    params = {
        "latitude":   [c["lat"] for c in communes],
        "longitude":  [c["lon"] for c in communes],
        "start_date": [start_date] * len(communes),
        "end_date":   [end_date]   * len(communes),
        "daily":      DAILY_VARIABLES_LOCAL,
        "models":     [ERA5_MODEL]  * len(communes),
        "timezone":   ["Europe/Paris"] * len(communes),
    }
    r = requests.post(LOCAL_API, json=params, timeout=120)
    if not r.ok:
        logger.error(f"ERA5 API : {r.text[:500]}")
    r.raise_for_status()
    results = r.json()
    return results if isinstance(results, list) else [results]


def fetch_local_era5(
    communes: pl.DataFrame,
    start_date: str,
    end_date: str,
    test: bool = False,
) -> pl.DataFrame:
    """Récupère les données ERA5 locales en batches POST."""
    rows = communes.select(["code_insee", "latitude", "longitude"]).to_dicts()
    commune_list = [
        {"code_insee": r["code_insee"], "lat": r["latitude"], "lon": r["longitude"]}
        for r in rows
    ]

    if test:
        commune_list = commune_list[:10]
        logger.info(f"  ⚠ Mode test : {len(commune_list)} communes")

    total     = len(commune_list)
    n_batches = (total + ERA5_BATCH - 1) // ERA5_BATCH
    records   = []

    for i in range(0, total, ERA5_BATCH):
        batch     = commune_list[i:i + ERA5_BATCH]
        batch_num = i // ERA5_BATCH + 1
        logger.info(f"  ERA5 batch {batch_num}/{n_batches} ({len(batch)} communes)")

        results = _fetch_era5_batch(batch, start_date, end_date)

        for commune, meteo in zip(batch, results):
            daily = meteo.get("daily", {})
            dates = daily.get("time", [])
            n     = len(dates)
            for j, d in enumerate(dates):
                rec = {"code_insee": commune["code_insee"], "date": d}
                for var in DAILY_VARIABLES_LOCAL:
                    # En multi-communes, l'API ajoute le suffixe _era5
                    val = daily.get(f"{var}_era5", daily.get(var, [None] * n))
                    rec[var] = val[j]
                records.append(rec)

        time.sleep(0.1)

    df = pl.DataFrame(records)
    df = df.with_columns(pl.col("date").str.to_date())
    logger.info(f"  ERA5 : {df.shape[0]} lignes récupérées")
    return df


# ====================================================================
# Source 2 : Archive distante (Données manquantes entre ERA5 -> hier)
# ====================================================================
def fetch_remote_archive(
    communes: pl.DataFrame,
    start_date: str,
    end_date: str,
) -> pl.DataFrame:
    """Récupère les archives Open-Meteo distantes pour la plage donnée."""
    return fetch_all_communes(communes, METEO_URL_ARCHIVES, {
        "start_date": start_date,
        "end_date":   end_date,
        "daily":      DAILY_VARIABLES,
        "timezone":   "Europe/Paris",
    }, label="archive distante")


# ============================================================
# Source 3 : Prévisions (J+7)
# ============================================================
def fetch_forecast(communes: pl.DataFrame, nb_jours: int = 7) -> pl.DataFrame:
    """Récupère les prévisions Open-Meteo/MétéoFrance."""
    return fetch_all_communes(communes, METEO_URL_PREVISIONS, {
        "forecast_days": nb_jours,
        "daily":         DAILY_VARIABLES,
        "timezone":      "Europe/Paris",
    }, label="prévisions")


# ============================================================
# Sauvegarde
# ============================================================
def save_historique(meteo: pl.DataFrame, annee: int):
    """Sauvegarde les données historiques normalisées (ERA5 + gap)."""
    path = METEO_DIR / f"historique/{annee}"
    path.mkdir(parents=True, exist_ok=True)
    out = path / "meteo.parquet"
    meteo.write_parquet(out)
    logger.info(f"✓ Historique sauvegardé : {out} ({meteo.shape[0]} lignes, {meteo['time'].min()} → {meteo['time'].max()})")


def save_previsions(meteo: pl.DataFrame):
    """Sauvegarde les prévisions brutes (écrase le fichier précédent)."""
    path = METEO_DIR / "previsions"
    path.mkdir(parents=True, exist_ok=True)
    out = path / "meteo_previsions.parquet"
    meteo.write_parquet(out)
    logger.info(f"✓ Prévisions sauvegardées : {out} ({meteo.shape[0]} lignes, {meteo['time'].min()} → {meteo['time'].max()})")


# ============================================================
# Pipeline principal
# ============================================================
def run(
    annee: int,
    region: str | None = None,
    mode_local: bool = True,
    nb_jours_forecast: int = 7,
    test: bool = False,
):
    """
    Pipeline météo en 3 couches :
      1. ERA5 local (Docker) — données depuis le 1er janvier, sans limite de requêtes
      2. Archive distante    — comble le gap entre la dernière date ERA5 et hier
      3. Prévisions          — J+0 à J+nb_jours_forecast

    Les données brutes normalisées sont sauvegardées pour être utilisées par
    calcul_risque_journalier.py qui se charge du calcul des indicateurs.
    """
    today         = date.today()
    yesterday     = today - timedelta(days=1)
    start_of_year = date(annee, 1, 1)

    logger.info(
        f"ETL Météo — {annee} | région : {region or 'toutes'} | "
        f"mode : {'local (ERA5)' if mode_local else 'distant'}"
    )

    # ── Chargement des communes ──────────────────────────────────────────────
    communes = load_communes(region)

    # ── 1. ERA5 local ────────────────────────────────────────────────────────
    era5_df   = None
    gap_start = start_of_year

    if mode_local:
        if not check_docker_era5():
            logger.warning(
                "Docker ERA5 indisponible — bascule automatique en mode distant.\n"
                "Pour lancer Docker : docker run -d --rm -v open-meteo-data:/app/data "
                "-p 8080:8080 ghcr.io/open-meteo/open-meteo"
            )
            mode_local = False
        else:
            logger.info(f"[1/3] ERA5 local : {start_of_year} → {today}...")
            era5_df   = normaliser_colonnes_meteo(fetch_local_era5(communes, str(start_of_year), str(today), test=test))
            # Ignorer les dates de fin où les données sont null (ERA5 lag)
            era5_df   = era5_df.filter(pl.col("precipitation_sum").is_not_null())
            last_era5 = era5_df["time"].cast(pl.Date).max() if not era5_df.is_empty() else None
            logger.info(f"  Dernière date ERA5 complète : {last_era5}")
            gap_start = (last_era5 + timedelta(days=1)) if last_era5 else start_of_year

    if not mode_local:
        logger.info("[1/3] Mode distant — ERA5 local ignoré")

    # ── 2. Archive distante (gap) ────────────────────────────────────────────
    archive_df = None
    if gap_start <= yesterday:
        logger.info(f"[2/3] Archive distante : {gap_start} → {yesterday}...")
        archive_df = normaliser_colonnes_meteo(fetch_remote_archive(communes, str(gap_start), str(yesterday)))
        logger.info(f"  Archive distante : {archive_df.shape[0]} lignes")
    else:
        logger.info("[2/3] Pas de gap à combler (ERA5 couvre jusqu'à hier)")

    # ── 3. Prévisions ────────────────────────────────────────────────────────
    logger.info(f"[3/3] Prévisions : {nb_jours_forecast} jours...")
    forecast_df = normaliser_colonnes_meteo(fetch_forecast(communes, nb_jours_forecast))

    # ── Fusion et sauvegarde ────────────────────────────────────────────────
    parts = [df for df in [era5_df, archive_df] if df is not None]
    if parts:
        historique = pl.concat(parts).sort(["code_insee", "time"])
        save_historique(historique, annee)
    else:
        logger.warning("Aucune donnée historique récupérée — fichier historique non mis à jour.")

    save_previsions(forecast_df)
    logger.info("ETL Météo terminé ✓")


if __name__ == "__main__":
    import argparse
    from datetime import datetime

    parser = argparse.ArgumentParser(description="ETL météo PestiExpo — ERA5 local + gap distant + prévisions")
    parser.add_argument("--annee",             type=int,  default=datetime.today().year,
                        help="Année cible (défaut : année courante)")
    parser.add_argument("--region",            type=str,  default=None,
                        help='Filtrer sur une région (ex: "Pays de la Loire")')
    parser.add_argument("--distant",           action="store_true",
                        help="Mode distant uniquement (pas de Docker ERA5)")
    parser.add_argument("--nb_jours_forecast", type=int,  default=7,
                        help="Nombre de jours de prévision (max 16, défaut : 7)")
    parser.add_argument("--test",              action="store_true",
                        help="Tester sur 10 communes seulement (mode local uniquement)")
    args = parser.parse_args()

    run(
        annee=args.annee,
        region=args.region,
        mode_local=not args.distant,
        nb_jours_forecast=args.nb_jours_forecast,
        test=args.test,
    )
