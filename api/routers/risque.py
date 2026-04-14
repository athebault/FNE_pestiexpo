from datetime import date
from typing import Optional

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from api.db import communes_ref, get_duckdb_con, annees_disponibles, DUCKDB_PATH
from api.schemas import CarteDateResponse, RisqueCommuneCarte, RisqueJour, RisqueSerie

router = APIRouter()


def _safe(val):
    return None if pd.isna(val) else val


def _has_risque_table(con, table: str = "risque_journalier") -> bool:
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    return table in tables


# ── Routes statiques en premier (avant /{code_insee}) ────────
# IMPORTANT : FastAPI matche les routes dans l'ordre de déclaration.
# Toutes les routes à segment fixe (/annees, /carte/..., /previsions/...)
# doivent être déclarées AVANT /{code_insee} pour éviter les conflits.

@router.get("/annees", response_model=list[int], summary="Années disponibles")
def get_annees():
    return annees_disponibles()


# ── Carte (historique) ────────────────────────────────────────

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

    if not DUCKDB_PATH.exists():
        raise HTTPException(503, "Base de données non disponible")

    con = get_duckdb_con()
    try:
        if not _has_risque_table(con):
            raise HTTPException(404, "Aucune donnée de risque disponible")
        df_risque = con.execute("""
            SELECT insee_com, risque_0_4, ift_journalier_total,
                   interdiction_pulv, pluie_limitante, risque_dispersion
            FROM risque_journalier
            WHERE date = ?
        """, [d]).df()
    finally:
        con.close()

    communes = communes_ref()
    if region:
        communes = communes[communes["code_insee_reg"] == region]
    if departement:
        communes = communes[communes["code_insee_dep"] == departement]

    merged = communes.merge(df_risque, left_on="code_insee", right_on="insee_com", how="left")

    result = [
        RisqueCommuneCarte(
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
        )
        for _, r in merged.iterrows()
    ]
    return CarteDateResponse(date=d, communes=result)


# ── Série temporelle commune (historique) ─────────────────────

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
    if not DUCKDB_PATH.exists():
        raise HTTPException(503, "Base de données non disponible")

    communes = communes_ref()
    commune_row = communes[communes["code_insee"] == code_insee]
    if commune_row.empty:
        raise HTTPException(404, f"Commune {code_insee} introuvable")
    info = commune_row.iloc[0]

    con = get_duckdb_con()
    try:
        if not _has_risque_table(con):
            raise HTTPException(404, "Aucune donnée de risque disponible")

        params = [code_insee, annee]
        where = "WHERE insee_com = ? AND year(date) = ?"
        if date_debut:
            where += " AND date >= ?"
            params.append(date_debut)
        if date_fin:
            where += " AND date <= ?"
            params.append(date_fin)

        df = con.execute(f"""
            SELECT date, risque_0_4, ift_journalier_total, risque_brut,
                   indicateur_meteo, interdiction_pulv, pluie_limitante, risque_dispersion
            FROM risque_journalier
            {where}
            ORDER BY date
        """, params).df()
    finally:
        con.close()

    jours = [
        RisqueJour(
            date=r["date"].date() if hasattr(r["date"], "date") else r["date"],
            risque_0_4=int(r["risque_0_4"]) if pd.notna(r.get("risque_0_4")) else None,
            ift_journalier_total=_safe(r.get("ift_journalier_total")),
            risque_brut=_safe(r.get("risque_brut")),
            indicateur_meteo=int(r["indicateur_meteo"]) if pd.notna(r.get("indicateur_meteo")) else None,
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


# ── Prévisions ────────────────────────────────────────────────

@router.get(
    "/previsions/dates",
    response_model=list[date],
    summary="Dates disponibles dans les prévisions météo",
)
def previsions_dates():
    if not DUCKDB_PATH.exists():
        return []
    con = get_duckdb_con()
    try:
        if not _has_risque_table(con, "risque_previsions"):
            return []
        rows = con.execute(
            "SELECT DISTINCT date FROM risque_previsions ORDER BY date"
        ).fetchall()
        return [r[0] for r in rows]
    finally:
        con.close()


@router.get(
    "/previsions/carte/{date_str}",
    response_model=CarteDateResponse,
    summary="Risque prévisionnel de toutes les communes pour une date",
)
def previsions_carte(
    date_str:    str,
    region:      Optional[str] = Query(None),
    departement: Optional[str] = Query(None),
):
    try:
        d = date.fromisoformat(date_str)
    except ValueError:
        raise HTTPException(400, "Format de date invalide (attendu : YYYY-MM-DD)")

    if not DUCKDB_PATH.exists():
        raise HTTPException(503, "Base de données non disponible")

    con = get_duckdb_con()
    try:
        if not _has_risque_table(con, "risque_previsions"):
            raise HTTPException(404, "Aucune prévision disponible")
        df_risque = con.execute("""
            SELECT insee_com, risque_0_4, ift_journalier_total,
                   interdiction_pulv, pluie_limitante, risque_dispersion
            FROM risque_previsions
            WHERE date = ?
        """, [d]).df()
    finally:
        con.close()

    if df_risque.empty:
        raise HTTPException(404, f"Aucune prévision pour le {d}")

    communes = communes_ref()
    if region:
        communes = communes[communes["code_insee_reg"] == region]
    if departement:
        communes = communes[communes["code_insee_dep"] == departement]

    merged = communes.merge(df_risque, left_on="code_insee", right_on="insee_com", how="left")

    result = [
        RisqueCommuneCarte(
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
        )
        for _, r in merged.iterrows()
    ]
    return CarteDateResponse(date=d, communes=result)


@router.get(
    "/previsions/{code_insee}",
    response_model=RisqueSerie,
    summary="Série prévisionnelle pour une commune",
)
def previsions_serie(code_insee: str):
    if not DUCKDB_PATH.exists():
        raise HTTPException(503, "Base de données non disponible")

    communes = communes_ref()
    commune_row = communes[communes["code_insee"] == code_insee]
    if commune_row.empty:
        raise HTTPException(404, f"Commune {code_insee} introuvable")
    info = commune_row.iloc[0]

    con = get_duckdb_con()
    try:
        if not _has_risque_table(con, "risque_previsions"):
            raise HTTPException(404, "Aucune prévision disponible")
        df = con.execute("""
            SELECT date, risque_0_4, ift_journalier_total, risque_brut,
                   indicateur_meteo, interdiction_pulv, pluie_limitante, risque_dispersion
            FROM risque_previsions
            WHERE insee_com = ?
            ORDER BY date
        """, [code_insee]).df()
    finally:
        con.close()

    annee = df["date"].dt.year.iloc[0] if not df.empty else date.today().year

    jours = [
        RisqueJour(
            date=r["date"].date() if hasattr(r["date"], "date") else r["date"],
            risque_0_4=int(r["risque_0_4"]) if pd.notna(r.get("risque_0_4")) else None,
            ift_journalier_total=_safe(r.get("ift_journalier_total")),
            risque_brut=_safe(r.get("risque_brut")),
            indicateur_meteo=int(r["indicateur_meteo"]) if pd.notna(r.get("indicateur_meteo")) else None,
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
        annee=int(annee),
        jours=jours,
    )
