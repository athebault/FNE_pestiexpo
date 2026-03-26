"""
ETL Météo Prévisions - PestiExpo
Récupère les prévisions météo (7 jours) pour toutes les communes
et calcule les indicateurs de risque pesticides.

Lancement:
    # Toutes régions
    uv run python3 etl/etl_meteo_previsions.py

    # Filtrer sur une région
    uv run python3 etl/etl_meteo_previsions.py --region "Pays de la Loire"

    # Avec données brutes
    uv run python3 etl/etl_meteo_previsions.py --save_brut
"""

import polars as pl
import logging

from datetime import datetime

from config import (
    PARQUET_DIR, METEO_DIR, METEO_ENABLED,
    VENT_MAX, VENT_DISPERSION, PLUIE_SEUIL,
    METEO_URL_PREVISIONS, DAILY_VARIABLES,
    code_region_rpg
)
from utils import fetch_all_communes

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

NB_JOURS_FORECAST = 7  # max 16 avec Open-Meteo


# ============================================================
# 1. CALCUL DES INDICATEURS
# ============================================================
def compute_indicateurs(meteo: pl.DataFrame) -> pl.DataFrame:
    """Calcule les indicateurs de risque à partir des données météo brutes."""
    return meteo.with_columns([
        (pl.col("wind_speed_10m_max") >= VENT_MAX).alias("interdiction_pulv"),
        (pl.col("precipitation_sum") >= PLUIE_SEUIL).alias("pluie_limitant_dispersion"),
        (
            (pl.col("wind_speed_10m_max") >= VENT_DISPERSION) &
            (pl.col("wind_speed_10m_max") <  VENT_MAX)
        ).alias("risque_dispersion"),
    ])


def aggregate_indicateurs(meteo: pl.DataFrame) -> pl.DataFrame:
    """Agrège les indicateurs par commune sur toute la période prévisionnelle."""
    return (
        meteo.group_by("code_insee")
        .agg([
            pl.col("interdiction_pulv").sum().alias("nb_jours_interdiction"),
            pl.col("pluie_limitant_dispersion").sum().alias("nb_jours_pluie_limitante"),
            pl.col("risque_dispersion").sum().alias("nb_jours_dispersion"),
            pl.col("wind_speed_10m_max").mean().alias("vent_moyen"),
            pl.col("wind_speed_10m_max").max().alias("vent_max_obs"),
            pl.col("precipitation_sum").sum().alias("precip_totale"),
            pl.col("temperature_2m_max").mean().alias("temp_max_moyenne"),
            pl.col("temperature_2m_min").mean().alias("temp_min_moyenne"),
            pl.col("et0_fao_evapotranspiration").sum().alias("eto_total"),
            pl.col("time").min().alias("date_debut_prevision"),
            pl.col("time").max().alias("date_fin_prevision"),
            pl.len().alias("nb_jours_obs"),
        ])
        .sort("code_insee")
    )


# ============================================================
# 3. SAUVEGARDE
# Les prévisions écrasent le fichier précédent (pas d'historique)
# ============================================================
def save_previsions(meteo: pl.DataFrame, region: str | None = None):
    """Sauvegarde les données météo prévisionnelles brutes."""
    suffix = f"_{region.replace(' ', '_')}" if region else ""
    path = METEO_DIR / "previsions"
    path.mkdir(parents=True, exist_ok=True)
    meteo.write_parquet(path / f"meteo_previsions{suffix}.parquet")
    logger.info(f"✓ Prévisions brutes sauvegardées : {path}")


def save_indicateurs_previsions(indicateurs: pl.DataFrame, region: str | None = None):
    """Sauvegarde les indicateurs prévisionnels dans data/parquet/ — écrase à chaque run."""
    suffix = f"_{region.replace(' ', '_')}" if region else ""
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)
    out = PARQUET_DIR / f"indicateurs_previsions{suffix}.parquet"
    indicateurs.write_parquet(out)
    logger.info(f"✓ Indicateurs prévisionnels sauvegardés : {out} ({indicateurs.shape[0]} communes)")
    logger.info(f"  Mise à jour : {datetime.today().strftime('%Y-%m-%d %H:%M')}")


# ============================================================
# 4. PIPELINE PRINCIPAL
# ============================================================
def run(nb_jours: int = NB_JOURS_FORECAST, save_brut: bool = False, region: str | None = None):
    """
    Pipeline prévisions :
    1. Charger les communes
    2. Récupérer les prévisions
    3. Calculer les indicateurs
    4. Sauvegarder (écrase le précédent)
    """
    logger.info(f"ETL Météo Prévisions — {nb_jours} jours ({datetime.today().strftime('%Y-%m-%d %H:%M')})")

    if not METEO_ENABLED:
        logger.info("METEO_ENABLED=False : ETL météo prévisions ignoré")
        return pl.DataFrame([])

    # 1. Communes
    communes = pl.read_parquet(PARQUET_DIR / "communes_admin.parquet")

    if region is not None:
        if region not in code_region_rpg:
            regions_valides = ", ".join(sorted(code_region_rpg.keys()))
            raise ValueError(f"Région inconnue : '{region}'. Régions valides : {regions_valides}")
        code_reg = code_region_rpg[region]
        communes = communes.filter(pl.col("code_insee_reg") == code_reg)
        logger.info(f"  Filtre région : {region} (code {code_reg}) — {communes.shape[0]} communes")
    else:
        logger.info(f"  {communes.shape[0]} communes chargées (toutes régions)")

    if communes.is_empty():
        raise ValueError(f"Aucune commune trouvée pour la région '{region}'.")

    # 2. Prévisions brutes
    meteo = fetch_all_communes(communes, METEO_URL_PREVISIONS, {
        "forecast_days": nb_jours,
        "daily":         DAILY_VARIABLES,
        "timezone":      "Europe/Paris",
    }, label="prévisions")
    logger.info(f"  {meteo.shape[0]} lignes météo récupérées")

    # 3. Indicateurs
    meteo_avec_indicateurs = compute_indicateurs(meteo)
    indicateurs = aggregate_indicateurs(meteo_avec_indicateurs)

    # 4. Sauvegarde
    if save_brut:
        save_previsions(meteo_avec_indicateurs, region)
    save_indicateurs_previsions(indicateurs, region)

    logger.info("ETL Météo Prévisions terminé ✓")
    return indicateurs


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--nb_jours",  type=int,  default=NB_JOURS_FORECAST,
                        help="Nombre de jours de prévision (max 16)")
    parser.add_argument("--save_brut", action="store_true",
                        help="Sauvegarder aussi les données brutes journalières")
    parser.add_argument("--region",    type=str,  default=None,
                        help='Filtrer sur une région (ex: "Pays de la Loire")')
    args = parser.parse_args()
    run(nb_jours=args.nb_jours, save_brut=args.save_brut, region=args.region)