"""
Init DuckDB - PestiExpo
Crée les vues sur les fichiers parquet pour requêtes SQL unifiées.
"""

import duckdb
import logging
from config import DUCKDB_PATH, PARQUET_DIR

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

    # Indicateurs météo — une vue par année disponible
    for f in sorted(PARQUET_DIR.glob("indicateurs_meteo_*.parquet")):
        annee = f.stem.split("_")[-1]
        vues[f"indicateurs_meteo_{annee}"] = f

    # Météo brute partitionnée
    meteo_hist = PARQUET_DIR.parent / "meteo/historique"
    if meteo_hist.exists():
        vues["meteo_historique"] = meteo_hist

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
    annees = [f.stem.split("_")[-1] for f in sorted(PARQUET_DIR.glob("indicateurs_meteo_*.parquet"))]
    if annees:
        union = " UNION ALL ".join([
            f"SELECT *, {a} AS annee FROM read_parquet('{PARQUET_DIR}/indicateurs_meteo_{a}.parquet')"
            for a in annees
        ])
        con.execute(f"CREATE OR REPLACE VIEW indicateurs_meteo AS {union}")
        logger.info(f"  ✓ Vue consolidée : indicateurs_meteo ({', '.join(annees)})")

    # Vue principale joignant tout
    con.execute("""
        CREATE OR REPLACE VIEW pestiexpo AS
        SELECT
            i.insee_com,
            i.nom,
            i.c_maj,
            i.c_ift_hbc,
            i.c_ift_h,
            i.groupe_maj,
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
        count = con.execute(f"SELECT COUNT(*) FROM {t[0]}").fetchone()[0]
        logger.info(f"  {t[0]:40s} {count:>10,} lignes")

    con.close()
    logger.info("\nDuckDB initialisé ✓")


if __name__ == "__main__":
    init_duckdb()