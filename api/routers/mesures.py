from datetime import date
from typing import Optional
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from api.db import get_con, mesures_path
from api.schemas import MesureRecord

router = APIRouter()


@router.get(
    "/{code_insee}",
    response_model=list[MesureRecord],
    summary="Mesures de pesticides atmosphériques pour une station",
)
def get_mesures(
    code_insee: str,
    substance:  Optional[str] = Query(None, description="Filtre par substance active"),
    date_debut: Optional[date] = Query(None),
    date_fin:   Optional[date] = Query(None),
    detecte_uniquement: bool   = Query(False, description="Uniquement les détections > 0"),
    limit:      int            = Query(500, le=5000),
):
    p = mesures_path()
    if not p.exists():
        raise HTTPException(
            404,
            "Données de mesures non disponibles. Lancez etl/etl_mesures_pesticides.py.",
        )

    where = [f"code_insee = '{code_insee}'"]
    if substance:
        where.append(f"substance ILIKE '%{substance}%'")
    if date_debut:
        where.append(f"debut_prelevement >= '{date_debut}'")
    if date_fin:
        where.append(f"debut_prelevement <= '{date_fin}'")
    if detecte_uniquement:
        where.append("detecte = true")

    con = get_con()
    try:
        df = con.execute(f"""
            SELECT code_insee, nom_commune, substance,
                   debut_prelevement, fin_prelevement,
                   annee, semaine,
                   concentration_ng_m3, detecte
            FROM read_parquet('{p}')
            WHERE {" AND ".join(where)}
            ORDER BY debut_prelevement, substance
            LIMIT {limit}
        """).df()
    finally:
        con.close()

    def _d(val):
        return None if pd.isna(val) else val

    return [
        MesureRecord(
            code_insee=_d(r.get("code_insee")),
            nom_commune=_d(r.get("nom_commune")),
            substance=_d(r.get("substance")),
            debut_prelevement=r["debut_prelevement"] if pd.notna(r.get("debut_prelevement")) else None,
            fin_prelevement=r["fin_prelevement"] if pd.notna(r.get("fin_prelevement")) else None,
            annee=int(r["annee"]) if pd.notna(r.get("annee")) else None,
            semaine=int(r["semaine"]) if pd.notna(r.get("semaine")) else None,
            concentration_ng_m3=_d(r.get("concentration_ng_m3")),
            detecte=bool(r["detecte"]) if pd.notna(r.get("detecte")) else None,
        )
        for _, r in df.iterrows()
    ]


@router.get(
    "",
    response_model=list[str],
    summary="Liste des stations avec mesures disponibles",
)
def list_stations():
    p = mesures_path()
    if not p.exists():
        raise HTTPException(404, "Données de mesures non disponibles.")
    con = get_con()
    try:
        return con.execute(
            f"SELECT DISTINCT code_insee FROM read_parquet('{p}') ORDER BY code_insee"
        ).df()["code_insee"].tolist()
    finally:
        con.close()
