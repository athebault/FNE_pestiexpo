"""
ETL Statique - PestiExpo
Alimentation des données statiques (mise à jour annuelle) :
- Communes + centroïdes
- IFT par commune (ADONIS)
- Calendrier d'épandage
- Nomenclature RPG

Lancement : 
    # Sans details (défaut)
    uv run python3 etl/etl_statique.py --annee 2026

    # Avec details
    uv run python3 etl/etl_statique.py --annee 2026 --details
"""

import polars as pl
import geopandas as gpd
import pandas as pd
from pathlib import Path
import logging
from config import PARQUET_DIR, COMMUNE_GPKG, RPG_GPKG, IFT_CSV, CALENDRIER_XLSX, NOMENCLATURE_XLSX

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ============================================================
# 1a. COMMUNES + CENTROIDES
# ============================================================
def build_communes() -> pl.DataFrame:
    logger.info("Chargement des communes...")
    
    # Chargement des données
    communes = gpd.read_file(COMMUNE_GPKG, layer="COMMUNE")
    
    # Centroïdes en LAMB93 puis reprojection en WGS84
    communes["longitude"] = communes.to_crs(epsg=2154).geometry.centroid.to_crs(epsg=4326).x
    communes["latitude"]  = communes.to_crs(epsg=2154).geometry.centroid.to_crs(epsg=4326).y
    
    # Garder uniquement les colonnes utiles
    cols = [
        "code_insee", "nom_officiel_en_majuscules", 
        "code_insee_du_departement", "code_insee_de_la_region", 
        "longitude", "latitude"]
    
    df_commune_admin = pl.from_pandas(
        communes[cols].rename(columns={
            "code_insee": "code_insee",
            "nom_officiel_en_majuscules": "nom_commune",
            "code_insee_du_departement": "code_insee_dep",
            "code_insee_de_la_region": "code_insee_reg",
        })
    )
    
    # 
    logger.info(f"  → {df_commune_admin.shape[0]} communes chargées")


    return df_commune_admin


# ============================================================
# 1b. DESCRIPTION DES COMMUNES
# ============================================================
def describe_communes() -> pl.DataFrame:
    logger.info("Données descriptives sur les communes...")

    # Chargement des données
    communes = gpd.read_file(COMMUNE_GPKG, layer="COMMUNE")
    rpg      = gpd.read_file(RPG_GPKG)

    # Jointure spatiale parcelles → communes
    rpg_avec_admin = gpd.sjoin(
        rpg,
        communes,
        how="left",
        predicate="within",
    ) 

    # Agrégation par commune 
    df_commune_details = (
        pl.from_pandas(pd.DataFrame(rpg_avec_admin.drop(columns="geometry")))
        .group_by("nom_officiel_en_majuscules")
        .agg([
            pl.col("surf_parc").sum().alias("SAU_tot"),
            pl.col("id_parcel").n_unique().alias("nb_parc"),
            pl.col("code_cultu").n_unique().alias("nb_cultures"),
            pl.col("code_group").n_unique().alias("nb_groupes_culture"),
        ])
        .rename({"nom_officiel_en_majuscules": "nom_commune"})
        .sort("nom_commune")
    )

    logger.info(f"  → {df_commune_details.shape[0]} communes traitées")
    return df_commune_details

# ============================================================
# 2. IFT COMMUNES (ADONIS)
# ============================================================
def build_ift() -> pl.DataFrame:
    logger.info("Chargement IFT ADONIS...")

    ift = pd.read_csv(IFT_CSV, sep=";", low_memory=False, dtype={"insee_com": str})

    # Colonnes utiles
    cols = ["id", "insee_com", "sau", "sau_bio",
            "p_bio", "p_bc", "p_sau",
            "c_maj", "c_ift_hbc", "c_ift_h",
            "cod_c_maj", "cod_c_hbc", "cod_c_h",
            "ift_t", "ift_t_hbc", "ift_h",
            "ift_t_hh", "ift_hh_hbc", "iftbc",
            ]
    
    ift = ift[cols].copy()

    # Suppression des doublons par commune (le CSV ADONIS peut avoir plusieurs lignes
    # pour la même commune : cultures ex-æquo au rang 1, ou vraie duplication).
    # On garde la première occurrence — les valeurs IFT sont identiques entre les doublons.
    n_avant = len(ift)
    ift = ift.drop_duplicates(subset=["insee_com"])
    n_apres = len(ift)
    if n_avant != n_apres:
        logger.warning(f"  ⚠ {n_avant - n_apres} doublon(s) supprimé(s) dans le CSV ADONIS ({n_avant} → {n_apres} lignes)")

    # Libellés cultures
    nomenclature = pl.read_excel(NOMENCLATURE_XLSX, sheet_name="Annexe_A_cultures")
    code_to_libelle = dict(zip(
        nomenclature["code_culture"].to_list(),
        nomenclature["libelle_culture"].to_list(),
    ))

    ift["c_maj_depuis_code"]     = ift["cod_c_maj"].map(code_to_libelle)
    ift["c_ift_hbc_depuis_code"] = ift["cod_c_hbc"].map(code_to_libelle)
    ift["c_ift_h_depuis_code"]   = ift["cod_c_h"].map(code_to_libelle)

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
          .alias("Debut_de_periode"),
        pl.date(annee, pl.col("Fin de période").dt.month(), pl.col("Fin de période").dt.day())
          .alias("Fin_de_periode"),
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
# 5. GEOMETRIES SIMPLIFIEES POUR LA CARTE
# ============================================================
def build_communes_geo() -> None:
    """
    Exporte les contours communaux simplifiés en GeoJSON pour Plotly Choroplethmapbox.
    Simplification ~100 m (tolerance=0.001°) pour limiter le poids du fichier.
    """
    logger.info("Export géométries communes simplifiées...")
    communes = gpd.read_file(COMMUNE_GPKG, layer="COMMUNE")
    communes = communes[["code_insee", "geometry"]].copy()
    communes["geometry"] = communes["geometry"].simplify(tolerance=0.001, preserve_topology=True)
    communes = communes[communes["geometry"].notna() & ~communes["geometry"].is_empty]
    out = PARQUET_DIR / "communes_geo.geojson"
    communes.to_file(out, driver="GeoJSON")
    size_mo = out.stat().st_size / 1e6
    logger.info(f"✓ communes_geo.geojson ({size_mo:.1f} Mo, {len(communes)} communes)")


# ============================================================
# 6. SAUVEGARDE EN PARQUET
# ============================================================
def save_all(annee: int = 2026, details: bool = False):
    PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    # Communes
    communes = build_communes()
    communes.write_parquet(PARQUET_DIR / "communes_admin.parquet")
    logger.info(f"✓ communes_admin.parquet")

    if details == True:
        communes_details = describe_communes()
        communes_details.write_parquet(PARQUET_DIR / "communes_details.parquet")
        logger.info(f"✓ communes_details.parquet")
    else: 
        logger.info(f"✓ Pas de calcul détaillé sur les communes")

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

    # Géométries simplifiées pour la carte choroplèthe
    build_communes_geo()

    logger.info("ETL statique terminé ✓")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--annee", type=int, default=2026)
    parser.add_argument("--details", action="store_true", default=False,
                    help="Calculer les détails par commune (très lent)")
    args = parser.parse_args()
    save_all(annee=args.annee, details=args.details)