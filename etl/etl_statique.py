"""
ETL Statique - PestiExpo
Alimentation des données statiques (mise à jour annuelle) :
- Communes + centroïdes
- IFT par commune (ADONIS)
- Calendrier d'épandage
- Nomenclature RPG

Lancement : python etl/etl_statique.py --annee 2026  --region "Pays de la Loire"
"""

import polars as pl
import geopandas as gpd
import pandas as pd
from pathlib import Path
import logging
from config import PARQUET_DIR, DATA_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# CHEMINS SOURCE 
# ============================================================
COMMUNE_GPKG    = DATA_DIR / f"raw/ADE_4-0_GPKG_WGS84G_FRA-ED2026-02-16.gpkg"
IFT_CSV         = DATA_DIR / "raw/fre-324510908-adonis-ift-2022-v04112024.csv"
CALENDRIER_XLSX = DATA_DIR / "raw/calendrier_culture_harmonise.xlsx"
NOMENCLATURE_XLSX = DATA_DIR / "raw/RPG_nomenclatures.xlsx"


# ============================================================
# 1. COMMUNES + CENTROIDES
# ============================================================
def build_communes() -> pl.DataFrame:
    logger.info("Chargement des communes...")
    
    communes = gpd.read_file(COMMUNE_GPKG, layer="COMMUNE")
    
    # Centroïdes en LAMB93 puis reprojection en WGS84
    communes["longitude"] = communes.to_crs(epsg=2154).geometry.centroid.to_crs(epsg=4326).x
    communes["latitude"]  = communes.to_crs(epsg=2154).geometry.centroid.to_crs(epsg=4326).y
    
    # Garder uniquement les colonnes utiles
    cols = ["INSEE_COM", "NOM", "INSEE_DEP", "INSEE_REG", "longitude", "latitude"]
    df = pl.from_pandas(
        communes[cols].rename(columns={
            "INSEE_COM": "code_insee",
            "NOM":       "nom_commune",
            "INSEE_DEP": "code_insee_dep",
            "INSEE_REG": "code_insee_reg",
        })
    )
    
    logger.info(f"  → {df.shape[0]} communes chargées")
    return df


# ============================================================
# 2. IFT COMMUNES (ADONIS)
# ============================================================
def build_ift() -> pl.DataFrame:
    logger.info("Chargement IFT ADONIS...")

    ift = pd.read_csv(IFT_CSV, sep=";")

    # Colonnes utiles
    cols = ["id", "insee_com", "sau", "sau_bio",
            "cod_c_maj", "cod_c_hbc", "cod_c_h",
            "ift_t", "ift_t_hbc", "ift_h",
            "ift_t_hh", "ift_hh_hbc", "iftbc",
            "p_bio", "p_bc", "p_sau"]
    
    ift = ift[cols].copy()

    # Libellés cultures
    nomenclature = pl.read_excel(NOMENCLATURE_XLSX, sheet_name="Annexe_A_cultures")
    code_to_libelle = dict(zip(
        nomenclature["code_culture"].to_list(),
        nomenclature["libelle_culture"].to_list()
    ))

    ift["c_maj"]     = ift["cod_c_maj"].map(code_to_libelle)
    ift["c_ift_hbc"] = ift["cod_c_hbc"].map(code_to_libelle)
    ift["c_ift_h"]   = ift["cod_c_h"].map(code_to_libelle)

    # Code département depuis code_insee
    ift["code_insee_dep"] = ift["insee_com"].str[:2]

    df = pl.from_pandas(ift)
    logger.info(f"  → {df.shape[0]} communes IFT chargées")
    return df


# ============================================================
# 3. CALENDRIER D'EPANDAGE
# ============================================================
def build_calendrier(annee: int = 2025) -> pl.DataFrame:
    logger.info("Chargement calendrier épandage...")

    cal = pl.read_excel(CALENDRIER_XLSX, sheet_name="CVDL + PDL")

    # Normaliser les booléens
    cal = cal.with_columns([
        (pl.col("Herbicides").str.to_lowercase()   == "oui").alias("Herbicides"),
        (pl.col("Fongicides").str.to_lowercase()   == "oui").alias("Fongicides"),
        (pl.col("Insecticides").str.to_lowercase() == "oui").alias("Insecticides"),
    ])

    # Forcer l'année
    cal = cal.with_columns([
        pl.date(annee, pl.col("Début de période").dt.month(), pl.col("Début de période").dt.day())
          .alias("Début de période"),
        pl.date(annee, pl.col("Fin de période").dt.month(), pl.col("Fin de période").dt.day())
          .alias("Fin de période"),
    ])

    logger.info(f"  → {cal.shape[0]} périodes chargées")
    return cal


# ============================================================
# 4. NOMENCLATURE RPG
# ============================================================
def build_nomenclature() -> dict[str, pl.DataFrame]:
    logger.info("Chargement nomenclature RPG...")

    annexe_a = pl.read_excel(NOMENCLATURE_XLSX, sheet_name="Annexe_A_cultures")
    annexe_b = pl.read_excel(NOMENCLATURE_XLSX, sheet_name="Annexe_B_cultures_derobees")
    annexe_c = pl.read_excel(NOMENCLATURE_XLSX, sheet_name="Annexe_C_regions")

    logger.info(f"  → Annexe A: {annexe_a.shape[0]} | B: {annexe_b.shape[0]} | C: {annexe_c.shape[0]}")
    return {"annexe_a": annexe_a, "annexe_b": annexe_b, "annexe_c": annexe_c}


# ============================================================
# 5. SAUVEGARDE EN PARQUET
# ============================================================
def save_all(annee: int = 2025):
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Communes
    communes = build_communes()
    communes.write_parquet(PARQUET_DIR / "communes.parquet")
    logger.info(f"✓ communes.parquet")

    # IFT
    ift = build_ift()
    ift.write_parquet(PARQUET_DIR / "ift_communes.parquet")
    logger.info(f"✓ ift_communes.parquet")

    # Jointure communes + IFT
    ift_enrichi = ift.join(
        communes.select(["code_insee", "nom_commune", "code_insee_dep", 
                         "code_insee_reg", "longitude", "latitude"]),
        left_on="insee_com",
        right_on="code_insee",
        how="left"
    )
    ift_enrichi.write_parquet(PARQUET_DIR / "ift_communes_enrichi.parquet")
    logger.info(f"✓ ift_communes_enrichi.parquet")

    # Calendrier
    cal = build_calendrier(annee=annee)
    cal.write_parquet(PARQUET_DIR / "calendrier_epandage.parquet")
    logger.info(f"✓ calendrier_epandage.parquet")

    # Nomenclature
    nomenclatures = build_nomenclature()
    for nom, df in nomenclatures.items():
        df.write_parquet(PARQUET_DIR / f"nomenclature_{nom}.parquet")
        logger.info(f"✓ nomenclature_{nom}.parquet")

    logger.info("ETL statique terminé ✓")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--annee", type=int, default=2026)
    args = parser.parse_args()
    save_all(annee=args.annee)