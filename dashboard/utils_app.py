"""
Fonctions utilitaires du dashboard PestiExpo.
Chargement des données (DuckDB), préparation des données pour les graphiques,
et construction des figures Plotly.
"""

import json
import sys
import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from datetime import date as date_type
from pathlib import Path

# config_app ajoute etl/ au sys.path — doit être importé avant calcul_risque_journalier
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "etl"))
from config_app import DB_PATH, GEOJSON_PATH, STATES, STATE_ORDER, REG_NOMS
from calcul_risque_journalier import CULTURE_MAPPING


# ============================================================
# Chargement des données (DuckDB)
# ============================================================

def _con() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB en lecture seule."""
    return duckdb.connect(str(DB_PATH), read_only=True)


@st.cache_data(show_spinner="Chargement des communes…")
def load_communes() -> pd.DataFrame:
    """Charge communes + IFT + flag calendrier depuis DuckDB."""
    con = _con()

    df = con.execute("""
        SELECT
            c.code_insee, c.nom_commune, c.code_insee_dep, c.code_insee_reg,
            c.longitude, c.latitude,
            i.c_maj, i.c_ift_hbc, i.c_ift_h,
            i.code_insee_dep AS dep_ift
        FROM communes c
        LEFT JOIN ift_communes_enrichi i ON c.code_insee = i.insee_com
    """).df()

    cal_pairs = set(
        con.execute("SELECT CAST(departement_code AS VARCHAR), culture FROM calendrier_epandage")
        .fetchall()
    )
    con.close()

    old, new = list(CULTURE_MAPPING.keys()), list(CULTURE_MAPPING.values())
    mapping = dict(zip(old, new))

    df["c_maj_cal"]     = df["c_maj"].map(mapping)
    df["c_ift_hbc_cal"] = df["c_ift_hbc"].map(mapping)
    df["c_ift_h_cal"]   = df["c_ift_h"].map(mapping)

    def _has_cal(row):
        dep = str(row.get("code_insee_dep") or "")
        for col in ("c_maj_cal", "c_ift_hbc_cal", "c_ift_h_cal"):
            val = row.get(col)
            if pd.notna(val) and (dep, val) in cal_pairs:
                return True
        return False

    df["has_calendar_data"] = df.apply(_has_cal, axis=1)
    df["nom_region"]        = df["code_insee_reg"].map(REG_NOMS).fillna("Autre")
    return df


@st.cache_data(show_spinner="Chargement des géométries…")
def load_geojson() -> dict | None:
    if not GEOJSON_PATH.exists():
        return None
    with open(GEOJSON_PATH) as f:
        return json.load(f)


@st.cache_data(show_spinner="Chargement des indicateurs de risque…")
def load_risque(annee: int) -> pd.DataFrame | None:
    con = _con()
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if "risque_journalier" not in tables:
        con.close()
        return None
    df = con.execute(
        "SELECT * FROM risque_journalier WHERE year(date) = ?", [annee]
    ).df()
    con.close()
    if df.empty:
        return None
    df["date"]        = pd.to_datetime(df["date"])
    df["is_forecast"] = False
    return df


@st.cache_data(show_spinner="Chargement des prévisions…")
def load_risque_previsions() -> pd.DataFrame | None:
    con = _con()
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if "risque_previsions" not in tables:
        con.close()
        return None
    df = con.execute("SELECT * FROM risque_previsions").df()
    con.close()
    if df.empty:
        return None
    df["date"]        = pd.to_datetime(df["date"])
    df["is_forecast"] = True
    return df


def annees_disponibles() -> list[int]:
    con = _con()
    tables = [t[0] for t in con.execute("SHOW TABLES").fetchall()]
    if "risque_journalier" not in tables:
        con.close()
        return []
    rows = con.execute(
        "SELECT DISTINCT year(date) AS annee FROM risque_journalier ORDER BY annee"
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


# ============================================================
# Logique d'état
# ============================================================

def get_state(risque_0_4, ift_total, has_cal: bool) -> str | int:
    """Détermine l'état d'affichage d'une commune pour un jour donné."""
    if not has_cal:
        return "no_calendar"
    if pd.isna(risque_0_4):
        return "no_data"
    return int(risque_0_4)


# ============================================================
# Préparation des données carte
# ============================================================

def build_map_data(
    view: pd.DataFrame,
    risque: pd.DataFrame | None,
    previsions: pd.DataFrame | None,
    date_sel: date_type | None,
) -> tuple[pd.DataFrame, bool]:
    """
    Fusionne les données de risque avec les communes pour la date sélectionnée.
    Retourne (map_data, is_forecast_date).
    """
    map_data         = view.copy()
    is_forecast_date = False

    risk_cols = ["risque_0_4", "ift_journalier_total", "interdiction_pulv",
                 "pluie_limitante", "risque_dispersion"]

    if date_sel is not None:
        rj = None
        if previsions is not None:
            rj_prev = previsions[previsions["date"].dt.date == date_sel]
            if not rj_prev.empty:
                rj, is_forecast_date = rj_prev, True
        if rj is None and risque is not None:
            rj_hist = risque[risque["date"].dt.date == date_sel]
            if not rj_hist.empty:
                rj = rj_hist

        if rj is not None:
            map_data = map_data.merge(
                rj[["insee_com"] + risk_cols],
                left_on="code_insee", right_on="insee_com", how="left",
            )
        else:
            for col in risk_cols:
                map_data[col] = float("nan")
    else:
        for col in risk_cols:
            map_data[col] = float("nan")

    map_data["state"] = map_data.apply(
        lambda r: get_state(r["risque_0_4"], r.get("ift_journalier_total"), r["has_calendar_data"]),
        axis=1,
    )
    map_data["color"] = map_data["state"].map(lambda s: STATES.get(s, STATES["no_data"])[0])
    map_data["label"] = map_data["state"].map(lambda s: STATES.get(s, STATES["no_data"])[1])
    map_data["hover"] = map_data.apply(
        lambda r: (
            f"<b>{r['nom_commune']}</b> ({r['code_insee']})<br>"
            f"Dép. {r['code_insee_dep']} — {r.get('nom_region','')}<br>"
            + (f"Indicateur : {int(r['risque_0_4'])}<br>" if pd.notna(r.get('risque_0_4')) else "")
            + f"{r['label']}"
        ),
        axis=1,
    )
    return map_data, is_forecast_date


# ============================================================
# Figures Plotly
# ============================================================

def make_fig_map(
    map_data: pd.DataFrame,
    geojson: dict | None,
    commune_sel: str | None,
    reg_sel: str | None,
    dep_sel: str | None,
) -> go.Figure:
    fig = go.Figure()
    state_to_int = {s: i for i, s in enumerate(STATE_ORDER)}
    n = len(STATE_ORDER)

    colorscale = []
    for i, s in enumerate(STATE_ORDER):
        c = STATES[s][0]
        colorscale.append([i / n, c])
        colorscale.append([(i + 1) / n, c])

    map_data["state_int"] = map_data["state"].map(state_to_int).fillna(1)

    if geojson:
        fig.add_trace(go.Choroplethmap(
            geojson=geojson,
            locations=map_data["code_insee"],
            featureidkey="properties.code_insee",
            z=map_data["state_int"],
            colorscale=colorscale, zmin=0, zmax=n - 1,
            showscale=False,
            marker_opacity=0.8, marker_line_width=0.3,
            marker_line_color="rgba(255,255,255,0.4)",
            text=map_data["hover"], hoverinfo="text", showlegend=False,
        ))
        if commune_sel:
            sel = map_data[map_data["code_insee"] == commune_sel]
            if not sel.empty:
                fig.add_trace(go.Choroplethmap(
                    geojson=geojson,
                    locations=sel["code_insee"],
                    featureidkey="properties.code_insee",
                    z=sel["state_int"],
                    colorscale=colorscale, zmin=0, zmax=n - 1,
                    showscale=False, showlegend=False,
                    marker_opacity=1.0, marker_line_width=3,
                    marker_line_color="white",
                    text=sel["hover"], hoverinfo="text",
                ))
        for s in STATE_ORDER:
            clr, lbl = STATES[s]
            fig.add_trace(go.Scattermap(
                lat=[None], lon=[None], mode="markers",
                marker=dict(size=10, color=clr),
                name=lbl, showlegend=True,
            ))
    else:
        for s, (clr, lbl) in STATES.items():
            sub = map_data[map_data["state"] == s]
            if sub.empty:
                continue
            fig.add_trace(go.Scattermap(
                lat=sub["latitude"], lon=sub["longitude"], mode="markers",
                marker=dict(size=5, color=clr, opacity=0.85),
                text=sub["hover"], hoverinfo="text", name=lbl,
            ))

    center_lat = map_data["latitude"].mean() if not map_data.empty else 46.5
    center_lon = map_data["longitude"].mean() if not map_data.empty else 2.3
    zoom = 8 if dep_sel else (6 if reg_sel else 5)

    fig.update_layout(
        map_style="carto-positron",
        map=dict(center=dict(lat=center_lat, lon=center_lon), zoom=zoom),
        height=600,
        margin=dict(l=0, r=0, t=0, b=0),
        legend=dict(
            orientation="v", x=0.01, y=0.99,
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#ccc", borderwidth=1,
            font=dict(size=11),
        ),
    )
    return fig


def make_fig_ts(ts: pd.DataFrame) -> go.Figure:
    ts["state"]    = ts.apply(
        lambda r: get_state(r["risque_0_4"], r.get("ift_journalier_total"),
                            bool(r.get("has_calendar_data", True))),
        axis=1,
    )
    ts["color"]    = ts["state"].map(lambda s: STATES.get(s, STATES["no_data"])[0])
    ts["risque_y"] = ts["risque_0_4"].fillna(0)
    ts["hover_ts"] = ts.apply(
        lambda r: (
            f"{'⚡ Prévision — ' if r['is_forecast'] else ''}"
            f"{r['date'].strftime('%d/%m/%Y')}<br>"
            f"Indicateur : {int(r['risque_0_4']) if pd.notna(r['risque_0_4']) else '—'}<br>"
            + (f"IFT journalier : {r['ift_journalier_total']:.3f}<br>"
               if pd.notna(r.get('ift_journalier_total')) else "")
            + ("🚫 Vent fort " if r.get('interdiction_pulv') else "")
            + ("🌧 Pluie"      if r.get('pluie_limitante')   else "")
        ),
        axis=1,
    )

    ts_hist = ts[~ts["is_forecast"]]
    ts_prev = ts[ts["is_forecast"]]

    fig = go.Figure()
    if not ts_hist.empty:
        fig.add_trace(go.Bar(
            x=ts_hist["date"], y=ts_hist["risque_y"],
            marker_color=ts_hist["color"], opacity=0.9,
            text=ts_hist["hover_ts"], hovertemplate="%{text}<extra></extra>",
            name="Historique", showlegend=False,
        ))
    if not ts_prev.empty:
        fig.add_trace(go.Bar(
            x=ts_prev["date"], y=ts_prev["risque_y"],
            marker_color=ts_prev["color"], opacity=0.45,
            marker_line_width=1.5, marker_line_color="grey",
            text=ts_prev["hover_ts"], hovertemplate="%{text}<extra></extra>",
            name="Prévision ⚡", showlegend=True,
        ))

    fig.add_vline(
        x=str(date_type.today()), line_width=1.5,
        line_dash="dash", line_color="grey",
        annotation_text="Auj.", annotation_position="top right",
        annotation_font_size=10,
    )
    fig.update_layout(
        title="Évolution du risque",
        yaxis=dict(range=[0, 4.3], tickvals=[0, 1, 2, 3, 4]),
        xaxis=dict(tickformat="%b"),
        height=280,
        margin=dict(l=10, r=10, t=40, b=10),
        bargap=0,
        legend=dict(orientation="h", y=1.1, x=0),
    )
    return fig
