"""Modèles Pydantic pour les réponses de l'API."""
from __future__ import annotations
from datetime import date
from typing import Optional
from pydantic import BaseModel


class CommuneBase(BaseModel):
    code_insee:     str
    nom_commune:    str
    code_insee_dep: Optional[str]
    code_insee_reg: Optional[str]
    latitude:       Optional[float]
    longitude:      Optional[float]


class CommuneDetail(CommuneBase):
    has_calendar_data:   bool
    c_maj:               Optional[str]
    c_maj_cal:           Optional[str]
    c_ift_hbc:           Optional[str]
    c_ift_hbc_cal:       Optional[str]
    c_ift_h:             Optional[str]
    c_ift_h_cal:         Optional[str]
    ift_t:               Optional[float]
    ift_h:               Optional[float]
    ift_hh_hbc:          Optional[float]


class RisqueJour(BaseModel):
    date:                 date
    risque_0_4:           Optional[int]
    ift_journalier_total: Optional[float]
    risque_brut:          Optional[float]
    facteur_meteo:        Optional[float]
    interdiction_pulv:    Optional[bool]
    pluie_limitante:      Optional[bool]
    risque_dispersion:    Optional[bool]


class RisqueSerie(BaseModel):
    code_insee:       str
    nom_commune:      str
    has_calendar_data: bool
    annee:            int
    jours:            list[RisqueJour]


class RisqueCommuneCarte(BaseModel):
    code_insee:           str
    nom_commune:          str
    latitude:             Optional[float]
    longitude:            Optional[float]
    has_calendar_data:    bool
    risque_0_4:           Optional[int]
    ift_journalier_total: Optional[float]
    interdiction_pulv:    Optional[bool]
    pluie_limitante:      Optional[bool]
    risque_dispersion:    Optional[bool]


class CarteDateResponse(BaseModel):
    date:     date
    communes: list[RisqueCommuneCarte]


class PeriodeCalendrier(BaseModel):
    departement_code: int
    culture:          str
    debut:            date
    fin:              date
    herbicides:       bool
    fongicides:       bool
    insecticides:     bool


class MesureRecord(BaseModel):
    code_insee:       Optional[str]
    nom_commune:      Optional[str]
    substance:        Optional[str]
    debut_prelevement: Optional[date]
    fin_prelevement:  Optional[date]
    annee:            Optional[int]
    semaine:          Optional[int]
    concentration_ng_m3: Optional[float]
    detecte:          Optional[bool]
