from typing import Optional
from fastapi import APIRouter, Query
from api.db import get_con, PARQUET
from api.schemas import PeriodeCalendrier

router = APIRouter()


@router.get("", response_model=list[PeriodeCalendrier], summary="Calendrier d'épandage")
def get_calendrier(
    culture:          Optional[str] = Query(None, description="Filtre par culture (ex: Maïs)"),
    departement_code: Optional[int] = Query(None, description="Filtre par code département"),
):
    p = PARQUET / "calendrier_epandage.parquet"
    where_clauses = []
    if culture:
        where_clauses.append(f"culture ILIKE '%{culture}%'")
    if departement_code:
        where_clauses.append(f"departement_code = {departement_code}")
    where = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

    con = get_con()
    try:
        df = con.execute(f"""
            SELECT departement_code, culture,
                   Debut_de_periode AS debut, Fin_de_periode AS fin,
                   Herbicides AS herbicides,
                   Fongicides AS fongicides,
                   Insecticides AS insecticides
            FROM read_parquet('{p}')
            {where}
            ORDER BY departement_code, culture, debut
        """).df()
    finally:
        con.close()

    return [
        PeriodeCalendrier(
            departement_code=int(r["departement_code"]),
            culture=r["culture"],
            debut=r["debut"],
            fin=r["fin"],
            herbicides=bool(r["herbicides"]),
            fongicides=bool(r["fongicides"]),
            insecticides=bool(r["insecticides"]),
        )
        for _, r in df.iterrows()
    ]


@router.get("/cultures", response_model=list[str], summary="Cultures disponibles dans le calendrier")
def get_cultures():
    p = PARQUET / "calendrier_epandage.parquet"
    con = get_con()
    try:
        return con.execute(
            f"SELECT DISTINCT culture FROM read_parquet('{p}') ORDER BY culture"
        ).df()["culture"].tolist()
    finally:
        con.close()
