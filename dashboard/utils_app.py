"""
Fonctions utilitaires du dashboard PestiExpo.
Toutes les données sont récupérées via l'API FastAPI.
"""

import json
import gzip
import requests
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import folium
from folium import plugins

from datetime import date as date_type
from pathlib import Path

from config_app import API_URL, GEOJSON_PATH, STATES, STATE_ORDER, REG_NOMS


# ============================================================
# Appels API
# ============================================================

def _api_get(path: str, params: dict | None = None):
    """Effectue un GET sur l'API. Lève une exception si l'API est injoignable."""
    try:
        r = requests.get(f"{API_URL}{path}", params=params, timeout=30)
        r.raise_for_status()
        return r.json()
    except requests.ConnectionError:
        st.error(f"Impossible de joindre l'API ({API_URL}). Vérifiez qu'elle est démarrée.")
        st.stop()


# ============================================================
# Chargement des données
# ============================================================

@st.cache_data(show_spinner="Chargement des communes…")
def load_communes() -> pd.DataFrame:
    data = _api_get("/communes/all")
    df = pd.DataFrame(data)
    df["nom_region"] = df["code_insee_reg"].map(REG_NOMS).fillna("Autre")
    return df


@st.cache_data(show_spinner="Chargement des géométries…")
def load_geojson() -> dict | None:
    geojson_gz = Path(str(GEOJSON_PATH) + ".gz")
    if not geojson_gz.exists():
        return None
    
    with gzip.open(geojson_gz, 'rt', encoding='utf-8') as f:
        return json.load(f)


def annees_disponibles() -> list[int]:
    return _api_get("/risque/annees") or []


@st.cache_data(show_spinner="Chargement de la carte…")
def load_risque_carte(date_sel: date_type) -> pd.DataFrame | None:
    """Risque de toutes les communes pour une date (historique)."""
    try:
        data = _api_get(f"/risque/carte/{date_sel}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise
    df = pd.DataFrame(data["communes"])
    if df.empty:
        return None
    df["date"] = pd.to_datetime(data["date"])
    df["is_forecast"] = False
    return df


@st.cache_data(show_spinner="Chargement des prévisions…")
def load_previsions_dates() -> list[date_type]:
    dates = _api_get("/risque/previsions/dates") or []
    return [date_type.fromisoformat(d) for d in dates]


@st.cache_data(show_spinner="Chargement de la carte prévisionnelle…")
def load_previsions_carte(date_sel: date_type) -> pd.DataFrame | None:
    """Risque prévisionnel de toutes les communes pour une date."""
    try:
        data = _api_get(f"/risque/previsions/carte/{date_sel}")
    except requests.HTTPError as e:
        if e.response.status_code == 404:
            return None
        raise
    df = pd.DataFrame(data["communes"])
    if df.empty:
        return None
    df["date"] = pd.to_datetime(data["date"])
    df["is_forecast"] = True
    return df


@st.cache_data(show_spinner="Chargement de la série temporelle…")
def load_risque_serie(code_insee: str, annee: int) -> pd.DataFrame:
    """Série temporelle historique pour une commune."""
    try:
        data = _api_get(f"/risque/{code_insee}", params={"annee": annee})
    except requests.HTTPError:
        return pd.DataFrame()
    df = pd.DataFrame(data.get("jours", []))
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["is_forecast"] = False
    return df


@st.cache_data(show_spinner="Chargement des prévisions commune…")
def load_previsions_serie(code_insee: str) -> pd.DataFrame:
    """Série prévisionnelle pour une commune."""
    try:
        data = _api_get(f"/risque/previsions/{code_insee}")
    except requests.HTTPError:
        return pd.DataFrame()
    df = pd.DataFrame(data.get("jours", []))
    if not df.empty:
        df["date"] = pd.to_datetime(df["date"])
        df["is_forecast"] = True
    return df


# ============================================================
# Logique d'état
# ============================================================

def get_state(risque_0_4, ift_total, has_cal: bool) -> str | int:
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
    date_sel: date_type | None,
    dates_prev: set[date_type],
) -> tuple[pd.DataFrame, bool]:
    """
    Récupère les données de risque pour la date sélectionnée via l'API
    et les fusionne avec les communes affichées.
    Retourne (map_data, is_forecast_date).
    """
    map_data         = view.copy()
    is_forecast_date = False

    risk_cols = ["risque_0_4", "ift_journalier_total", "interdiction_pulv",
                 "pluie_limitante", "risque_dispersion"]

    rj = None
    if date_sel is not None:
        if date_sel in dates_prev:
            rj = load_previsions_carte(date_sel)
            if rj is not None:
                is_forecast_date = True
        if rj is None:
            rj = load_risque_carte(date_sel)

    if rj is not None and not rj.empty:
        map_data = map_data.merge(
            rj[["code_insee"] + risk_cols],
            on="code_insee",
            how="left",
        )
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
# Affichage carte 
# ============================================================
def _style_function(feature, color_map):
    """Applique le style (couleur) à une feature GeoJSON."""
    insee = feature['properties'].get('code_insee')
    color = color_map.get(insee, '#cccccc')  # Gris par défaut
    return {
        'fillColor': color,
        'color': '#333',
        'weight': 1,
        'opacity': 0.8,
        'fillOpacity': 0.7,
    }


def _get_popup(feature, map_data):
    """Génère le texte du popup pour une feature."""
    insee = feature['properties'].get('code_insee')
    row = map_data[map_data['code_insee'] == insee]
    if row.empty:
        return "—"
    r = row.iloc[0]
    return f"{r['nom_commune']} ({insee})<br>État : {r['label']}"


def make_map_folium(map_data: pd.DataFrame, geojson: dict | None, zoom: int = 6) -> folium.Map:
    """Crée une carte Folium avec les communes coloriées (polygones)."""
    
    center_lat = map_data["latitude"].mean()
    center_lon = map_data["longitude"].mean()
    
    m = folium.Map(
        location=[center_lat, center_lon],
        zoom_start=zoom,
        tiles="OpenStreetMap",
    )
    
    if geojson is None:
        return m
    
    # Créer un dictionnaire code_insee → couleur
    color_map = dict(zip(map_data["code_insee"], map_data["color"]))
    
    # Ajouter les géométries coloriées
    for feature in geojson.get('features', []):
        folium.GeoJson(
            feature,
            style_function=lambda f, cm=color_map: _style_function(f, cm),
            popup=folium.Popup(_get_popup(feature, map_data), max_width=300),
            tooltip=feature['properties'].get('code_insee', ''),
        ).add_to(m)
    
    return m

# ============================================================
# Figures 
# ============================================================
@st.cache_data(show_spinner="Chargement de la carte…")
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

    if geojson is not None:
        # Utiliser Choroplethmapbox pour afficher les géométries coloriées
        insee_to_state = dict(zip(map_data['code_insee'], map_data['state_int']))
        insee_to_hover = dict(zip(map_data['code_insee'], map_data['hover']))
        
        locations = [f['properties']['code_insee'] for f in geojson['features']]
        z = [insee_to_state.get(loc, 1) for loc in locations]
        hovertext = [insee_to_hover.get(loc, loc) for loc in locations]
        
        fig.add_trace(go.Choroplethmapbox(
            geojson=geojson,
            locations=locations,
            z=z,
            colorscale=colorscale,
            zmin=0,
            zmax=n-1,
            marker_opacity=0.7,
            marker_line_width=0.5,
            hovertext=hovertext,
            hovertemplate="%{hovertext}<extra></extra>",
            featureidkey="properties.code_insee",
            showscale=False,
        ))
    else:
        # Fallback to scatter markers if no GeoJSON
        fig.add_trace(go.Scattermapbox(
            lat=map_data["latitude"],
            lon=map_data["longitude"],
            mode="markers",
            marker=dict(
                size=12,
                color=map_data["state_int"],
                colorscale=colorscale,
                cmin=0,
                cmax=n-1,
                showscale=False,
            ),
            text=map_data["hover"],
            hoverinfo="text",
            showlegend=False,
        ))

    # Ajouter la légende
    for s in STATE_ORDER:
        clr, lbl = STATES[s]
        fig.add_trace(go.Scattermapbox(
            lat=[None], lon=[None], mode="markers",
            marker=dict(size=10, color=clr),
            name=lbl, showlegend=True,
        ))

    center_lat = map_data["latitude"].mean() if not map_data.empty else 46.5
    center_lon = map_data["longitude"].mean() if not map_data.empty else 2.3
    zoom = 8 if dep_sel else (6 if reg_sel else 5)

    fig.update_layout(
        mapbox=dict(
            style="carto-positron",
            center=dict(lat=center_lat, lon=center_lon),
            zoom=zoom,
        ),
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

    # Convert timestamp to numeric value for add_vline (avoids Plotly timestamp arithmetic bug)
    if not ts.empty and pd.notna(ts["date"].iloc[0]):
        vline_value = pd.Timestamp(ts["date"].iloc[0]).value  # Convert to nanoseconds
        fig.add_vline(
            x=vline_value, line_width=1.5,
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