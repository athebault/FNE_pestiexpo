"""
Gestion de la connexion DuckDB et chargement des données de référence.
Toutes les requêtes lisent directement les fichiers parquet via DuckDB.
"""
import os
import sys
from functools import lru_cache
from pathlib import Path

import duckdb
import pandas as pd
import polars as pl

ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR  = Path(os.getenv("DATA_DIR", str(ROOT / "data")))
PARQUET   = DATA_DIR / "parquet"

sys.path.insert(0, str(ROOT / "etl"))
from calcul_risque_journalier import CULTURE_MAPPING


def get_con() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB in-memory (lecture directe des parquets, sans lock)."""
    return duckdb.connect(":memory:")


@lru_cache(maxsize=1)
def communes_ref() -> pd.DataFrame:
    """
    Table de référence des communes avec :
    - coordonnées, région, département
    - cultures IFT normalisées
    - flag has_calendar_data
    Chargée une seule fois au démarrage.
    """
    communes = pl.read_parquet(PARQUET / "communes_admin.parquet")
    ift      = pl.read_parquet(PARQUET / "ift_communes_enrichi.parquet")
    cal      = pl.read_parquet(PARQUET / "calendrier_epandage.parquet")

    old, new = list(CULTURE_MAPPING.keys()), list(CULTURE_MAPPING.values())
    ift = ift.with_columns([
        pl.col("c_maj").replace_strict(old=old, new=new, default=None).alias("c_maj_cal"),
        pl.col("c_ift_hbc").replace_strict(old=old, new=new, default=None).alias("c_ift_hbc_cal"),
        pl.col("c_ift_h").replace_strict(old=old, new=new, default=None).alias("c_ift_h_cal"),
    ])

    cal_pairs = set(zip(
        cal["departement_code"].cast(pl.Utf8).to_list(),
        cal["culture"].to_list(),
    ))

    df = communes.join(
        ift.select([
            "insee_com", "code_insee_dep",
            "c_maj", "c_maj_cal",
            "c_ift_hbc", "c_ift_hbc_cal",
            "c_ift_h", "c_ift_h_cal",
            "ift_t", "ift_h", "ift_hh_hbc",
        ]),
        left_on="code_insee", right_on="insee_com", how="left",
    ).to_pandas()

    def _has_cal(row):
        dep = str(row.get("code_insee_dep") or "")
        for col in ("c_maj_cal", "c_ift_hbc_cal", "c_ift_h_cal"):
            val = row.get(col)
            if pd.notna(val) and (dep, val) in cal_pairs:
                return True
        return False

    df["has_calendar_data"] = df.apply(_has_cal, axis=1)
    return df


def risque_path(annee: int) -> Path:
    return PARQUET / f"risque_journalier_{annee}.parquet"


def mesures_path() -> Path:
    return PARQUET / "mesures_pesticides_meteo.parquet"


def annees_disponibles() -> list[int]:
    return sorted(
        int(f.stem.split("_")[-1])
        for f in PARQUET.glob("risque_journalier_*.parquet")
        if f.stem.split("_")[-1].isdigit()
    )
