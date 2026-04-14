from fastapi import APIRouter, HTTPException, Query
from typing import Optional
import pandas as pd

from api.db import communes_ref
from api.schemas import CommuneBase, CommuneDetail

router = APIRouter()


def _row_to_base(r) -> CommuneBase:
    return CommuneBase(
        code_insee=r["code_insee"],
        nom_commune=r["nom_commune"],
        code_insee_dep=r.get("code_insee_dep"),
        code_insee_reg=r.get("code_insee_reg"),
        latitude=r.get("latitude"),
        longitude=r.get("longitude"),
    )


def _row_to_detail(r) -> CommuneDetail:
    return CommuneDetail(
        **_row_to_base(r).model_dump(),
        has_calendar_data=bool(r.get("has_calendar_data", False)),
        c_maj=r.get("c_maj") if pd.notna(r.get("c_maj")) else None,
        c_maj_cal=r.get("c_maj_cal") if pd.notna(r.get("c_maj_cal")) else None,
        c_ift_hbc=r.get("c_ift_hbc") if pd.notna(r.get("c_ift_hbc")) else None,
        c_ift_hbc_cal=r.get("c_ift_hbc_cal") if pd.notna(r.get("c_ift_hbc_cal")) else None,
        c_ift_h=r.get("c_ift_h") if pd.notna(r.get("c_ift_h")) else None,
        c_ift_h_cal=r.get("c_ift_h_cal") if pd.notna(r.get("c_ift_h_cal")) else None,
        ift_t=r.get("ift_t") if pd.notna(r.get("ift_t")) else None,
        ift_h=r.get("ift_h") if pd.notna(r.get("ift_h")) else None,
        ift_hh_hbc=r.get("ift_hh_hbc") if pd.notna(r.get("ift_hh_hbc")) else None,
    )


@router.get("/all", response_model=list[CommuneDetail], summary="Toutes les communes avec détail IFT et calendrier")
def all_communes():
    df = communes_ref()
    return [_row_to_detail(r) for _, r in df.iterrows()]


@router.get("", response_model=list[CommuneBase], summary="Liste des communes")
def list_communes(
    region:      Optional[str] = Query(None, description="Code région (ex: 24)"),
    departement: Optional[str] = Query(None, description="Code département (ex: 37)"),
    q:           Optional[str] = Query(None, description="Recherche par nom"),
    limit:       int           = Query(200, le=5000),
    offset:      int           = Query(0, ge=0),
):
    df = communes_ref()
    if region:
        df = df[df["code_insee_reg"] == region]
    if departement:
        df = df[df["code_insee_dep"] == departement]
    if q:
        df = df[df["nom_commune"].str.contains(q.upper(), na=False)]
    df = df.iloc[offset: offset + limit]
    return [_row_to_base(r) for _, r in df.iterrows()]


@router.get("/search", response_model=list[CommuneBase], summary="Recherche par nom")
def search_communes(
    q:     str = Query(..., min_length=2, description="Début du nom de commune"),
    limit: int = Query(20, le=100),
):
    df = communes_ref()
    results = df[df["nom_commune"].str.contains(q.upper(), na=False)].head(limit)
    return [_row_to_base(r) for _, r in results.iterrows()]


@router.get("/{code_insee}", response_model=CommuneDetail, summary="Détail d'une commune")
def get_commune(code_insee: str):
    df = communes_ref()
    row = df[df["code_insee"] == code_insee]
    if row.empty:
        raise HTTPException(status_code=404, detail=f"Commune {code_insee} introuvable")
    return _row_to_detail(row.iloc[0])
