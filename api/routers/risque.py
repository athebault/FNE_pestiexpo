from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from api.db import communes_ref, get_con, risque_path, annees_disponibles, PARQUET
from api.schemas import CarteDateResponse, RisqueCommuneCarte, RisqueJour, RisqueSerie

router = APIRouter()


def _safe(val):
    return None if pd.isna(val) else val


@router.get("/annees", response_model=list[int], summary="Années disponibles")
def get_annees():
    return annees_disponibles()


@router.get(
    "/carte/{date_str}",
    response_model=CarteDateResponse,
    summary="Risque de toutes les communes pour une date donnée",
)
def carte(
    date_str:    str,
    region:      Optional[str] = Query(None, description="Filtre par code région"),
    departement: Optional[str] = Query(None, description="Filtre par code département"),
):
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(400, "Format de date invalide (attendu : YYYY-MM-DD)")

    annee = d.year
    p = risque_path(annee)
    if not p.exists():
        raise HTTPException(404, f"Aucune donnée de risque pour l'année {annee}")

    con = get_con()
    try:
        df_risque = con.execute(f"""
            SELECT insee_com, risque_0_4, ift_journalier_total,
                   interdiction_pulv, pluie_limitante, risque_dispersion
            FROM read_parquet('{p}')
            WHERE date = '{d}'
        """).df()
    finally:
        con.close()

    communes = communes_ref()
    if region:
        communes = communes[communes["code_insee_reg"] == region]
    if departement:
        communes = communes[communes["code_insee_dep"] == departement]

    merged = communes.merge(df_risque, left_on="code_insee", right_on="insee_com", how="left")

    result = []
    for _, r in merged.iterrows():
        result.append(RisqueCommuneCarte(
            code_insee=r["code_insee"],
            nom_commune=r["nom_commune"],
            latitude=_safe(r.get("latitude")),
            longitude=_safe(r.get("longitude")),
            has_calendar_data=bool(r.get("has_calendar_data", False)),
            risque_0_4=int(r["risque_0_4"]) if pd.notna(r.get("risque_0_4")) else None,
            ift_journalier_total=_safe(r.get("ift_journalier_total")),
            interdiction_pulv=_safe(r.get("interdiction_pulv")),
            pluie_limitante=_safe(r.get("pluie_limitante")),
            risque_dispersion=_safe(r.get("risque_dispersion")),
        ))

    return CarteDateResponse(date=d, communes=result)


@router.get(
    "/{code_insee}",
    response_model=RisqueSerie,
    summary="Série temporelle du risque pour une commune",
)
def serie_commune(
    code_insee: str,
    annee:      int            = Query(..., description="Année (ex: 2025)"),
    date_debut: Optional[date] = Query(None),
    date_fin:   Optional[date] = Query(None),
):
    p = risque_path(annee)
    if not p.exists():
        raise HTTPException(404, f"Aucune donnée de risque pour l'année {annee}")

    communes = communes_ref()
    commune_row = communes[communes["code_insee"] == code_insee]
    if commune_row.empty:
        raise HTTPException(404, f"Commune {code_insee} introuvable")
    info = commune_row.iloc[0]

    where = f"WHERE insee_com = '{code_insee}'"
    if date_debut:
        where += f" AND date >= '{date_debut}'"
    if date_fin:
        where += f" AND date <= '{date_fin}'"

    con = get_con()
    try:
        df = con.execute(f"""
            SELECT date, risque_0_4, ift_journalier_total, risque_brut,
                   facteur_meteo, interdiction_pulv, pluie_limitante, risque_dispersion
            FROM read_parquet('{p}')
            {where}
            ORDER BY date
        """).df()
    finally:
        con.close()

    jours = [
        RisqueJour(
            date=r["date"].date() if hasattr(r["date"], "date") else r["date"],
            risque_0_4=int(r["risque_0_4"]) if pd.notna(r.get("risque_0_4")) else None,
            ift_journalier_total=_safe(r.get("ift_journalier_total")),
            risque_brut=_safe(r.get("risque_brut")),
            facteur_meteo=_safe(r.get("facteur_meteo")),
            interdiction_pulv=_safe(r.get("interdiction_pulv")),
            pluie_limitante=_safe(r.get("pluie_limitante")),
            risque_dispersion=_safe(r.get("risque_dispersion")),
        )
        for _, r in df.iterrows()
    ]

    return RisqueSerie(
        code_insee=code_insee,
        nom_commune=info["nom_commune"],
        has_calendar_data=bool(info.get("has_calendar_data", False)),
        annee=annee,
        jours=jours,
    )


@router.get(
    "/{code_insee}/{date_str}",
    response_model=RisqueJour,
    summary="Risque d'une commune pour un jour donné",
)
def risque_jour(code_insee: str, date_str: str):
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(400, "Format de date invalide (attendu : YYYY-MM-DD)")

    p = risque_path(d.year)
    if not p.exists():
        raise HTTPException(404, f"Aucune donnée de risque pour l'année {d.year}")

    con = get_con()
    try:
        df = con.execute(f"""
            SELECT date, risque_0_4, ift_journalier_total, risque_brut,
                   facteur_meteo, interdiction_pulv, pluie_limitante, risque_dispersion
            FROM read_parquet('{p}')
            WHERE insee_com = '{code_insee}' AND date = '{d}'
        """).df()
    finally:
        con.close()

    if df.empty:
        raise HTTPException(404, f"Aucune donnée pour {code_insee} le {d}")

    r = df.iloc[0]
    return RisqueJour(
        date=d,
        risque_0_4=int(r["risque_0_4"]) if pd.notna(r.get("risque_0_4")) else None,
        ift_journalier_total=_safe(r.get("ift_journalier_total")),
        risque_brut=_safe(r.get("risque_brut")),
        facteur_meteo=_safe(r.get("facteur_meteo")),
        interdiction_pulv=_safe(r.get("interdiction_pulv")),
        pluie_limitante=_safe(r.get("pluie_limitante")),
        risque_dispersion=_safe(r.get("risque_dispersion")),
    )
