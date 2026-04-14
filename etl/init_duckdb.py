"""
Init DuckDB - PestiExpo
Crée les vues sur les fichiers parquet pour requêtes SQL unifiées.

Lancement: uv run python3 etl/init_duckdb.py
"""

import re
import duckdb
import logging
from etl.etl_config import DUCKDB_PATH, PARQUET_DIR, METEO_DIR, METEO_ENABLED

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def init_duckdb():
    logger.info(f"Initialisation DuckDB : {DUCKDB_PATH}")
    con = duckdb.connect(str(DUCKDB_PATH))

    vues = {
        # Données statiques
        "communes":              PARQUET_DIR / "communes_admin.parquet",
        "ift_communes":          PARQUET_DIR / "ift_communes.parquet",
        "ift_communes_enrichi":  PARQUET_DIR / "ift_communes_enrichi.parquet",
        "calendrier_epandage":   PARQUET_DIR / "calendrier_epandage.parquet",
        "nomenclature_cultures": PARQUET_DIR / "nomenclature_annexe_a.parquet",
        "nomenclature_derobees": PARQUET_DIR / "nomenclature_annexe_b.parquet",
        "nomenclature_regions":  PARQUET_DIR / "nomenclature_annexe_c.parquet",
    }

    if METEO_ENABLED:
        # Météo brute historique (hive partitioning)
        meteo_hist = METEO_DIR / "historique"
        if meteo_hist.exists():
            vues["meteo_historique"] = meteo_hist

        # Météo brute prévisionnelle
        meteo_prev = METEO_DIR / "previsions"
        if meteo_prev.exists():
            vues["meteo_previsions"] = meteo_prev
    else:
        logger.info("METEO_ENABLED=False : vues météo ignorées")

    # Indicateurs météo transformés (dans parquet/)
    for f in sorted(PARQUET_DIR.glob("indicateurs_meteo_*.parquet")):
        if re.fullmatch(r"indicateurs_meteo_\d{4}", f.stem):
            annee = f.stem.split("_")[-1]
            vues[f"indicateurs_meteo_{annee}"] = f

    for f in sorted(PARQUET_DIR.glob("indicateurs_previsions*.parquet")):
        vues[f.stem] = f

    # Créer les vues
    for nom, path in vues.items():
        if not path.exists():
            logger.warning(f"  ⚠ Fichier manquant, vue ignorée : {path}")
            continue

        # Vue sur dossier partitionné (hive partitioning)
        if path.is_dir():
            sql = f"CREATE OR REPLACE VIEW {nom} AS SELECT * FROM read_parquet('{path}/**/*.parquet', hive_partitioning=true)"
        else:
            sql = f"CREATE OR REPLACE VIEW {nom} AS SELECT * FROM read_parquet('{path}')"

        con.execute(sql)
        logger.info(f"  ✓ Vue créée : {nom}")

    # Vue consolidée indicateurs météo toutes années
    annees = [
        f.stem.split("_")[-1]
        for f in sorted(PARQUET_DIR.glob("indicateurs_meteo_*.parquet"))
        if re.fullmatch(r"indicateurs_meteo_\d{4}", f.stem)
    ]
    if annees:
        union = " UNION ALL ".join([
            f"SELECT *, {a} AS annee FROM read_parquet('{PARQUET_DIR}/indicateurs_meteo_{a}.parquet')"
            for a in annees
        ])
        con.execute(f"CREATE OR REPLACE VIEW indicateurs_meteo AS {union}")
        logger.info(f"  ✓ Vue consolidée : indicateurs_meteo ({', '.join(annees)})")

    # Vue consolidée indicateurs prévisionnels (toutes régions confondues)
    previsions_files = sorted(PARQUET_DIR.glob("indicateurs_previsions*.parquet"))
    if previsions_files:
        union_prev = " UNION ALL ".join([
            f"SELECT * FROM read_parquet('{f}')" for f in previsions_files
        ])
        con.execute(f"CREATE OR REPLACE VIEW indicateurs_previsions_all AS {union_prev}")
        logger.info(f"  ✓ Vue consolidée : indicateurs_previsions_all ({len(previsions_files)} fichier(s))")

    # Vue principale joignant tout
    con.execute("""
        CREATE OR REPLACE VIEW pestiexpo AS
        SELECT
            i.insee_com,
            i.nom_commune,
            i.c_maj,
            i.c_ift_hbc,
            i.c_ift_h,
            i.c_maj,
            i.ift_t,
            i.ift_t_hbc,
            i.ift_h,
            i.iftbc,
            i.p_bio,
            i.p_sau,
            c.nom_commune,
            c.code_insee_dep,
            c.code_insee_reg,
            c.latitude,
            c.longitude
        FROM ift_communes_enrichi i
        LEFT JOIN communes c ON i.insee_com = c.code_insee
    """)
    logger.info("  ✓ Vue principale : pestiexpo")

    # Vérification
    tables = con.execute("SHOW TABLES").fetchall()
    logger.info(f"\nVues disponibles ({len(tables)}) :")
    for t in tables:
        try:
            count = con.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
            logger.info(f"  {t[0]:40s} {count:>10,} lignes")
        except Exception as e:
            logger.warning(f"  {t[0]:40s} Erreur lors du comptage : {e}")

    con.close()
    logger.info("\nDuckDB initialisé ✓")


if __name__ == "__main__":
    init_duckdb()