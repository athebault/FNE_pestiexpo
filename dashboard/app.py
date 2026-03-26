"""
PestiExpo Dashboard — Streamlit
Indicateur journalier d'exposition aux pesticides par commune.

Lancement local :
    uv run streamlit run dashboard/app.py

Déploiement Streamlit Cloud :
    → Pousser le repo sur GitHub, connecter sur https://streamlit.io/cloud
"""

import sys
from pathlib import Path
import streamlit as st
import polars as pl
import pandas as pd
import plotly.graph_objects as go

ROOT    = Path(__file__).resolve().parent.parent
PARQUET = ROOT / "data" / "parquet"
sys.path.insert(0, str(ROOT / "etl"))

from calcul_risque_journalier import CULTURE_MAPPING

# ── Palette & états ─────────────────────────────────────────
#   Chaque clé = état affiché sur la carte
#   Différence explicite entre "pas de données calendrier" et "pas de traitement ce jour"
STATES: dict[str | int, tuple[str, str]] = {
    "no_calendar": ("#78909C", "Hors calendrier (culture non couverte)"),
    "no_data":     ("#CFD8DC", "Données de risque non disponibles"),
    0:             ("#A5D6A7", "Aucun traitement ce jour"),
    1:             ("#FFF176", "Risque faible"),
    2:             ("#FFB300", "Risque modéré"),
    3:             ("#F57C00", "Risque élevé"),
    4:             ("#B71C1C", "Risque très élevé"),
}

REG_NOMS = {
    "01": "Guadeloupe", "02": "Martinique", "03": "Guyane", "04": "La Réunion",
    "06": "Mayotte", "11": "Île-de-France", "24": "Centre-Val de Loire",
    "27": "Bourgogne-Franche-Comté", "28": "Normandie", "32": "Hauts-de-France",
    "44": "Grand Est", "52": "Pays de la Loire", "53": "Bretagne",
    "75": "Nouvelle-Aquitaine", "76": "Occitanie", "84": "Auvergne-Rhône-Alpes",
    "93": "Provence-Alpes-Côte d'Azur", "94": "Corse",
}


# ── Chargement des données ───────────────────────────────────

@st.cache_data(show_spinner="Chargement des communes…")
def load_communes() -> pd.DataFrame:
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

    df = (
        communes.join(
            ift.select([
                "insee_com", "code_insee_dep",
                "c_maj", "c_maj_cal",
                "c_ift_hbc", "c_ift_hbc_cal",
                "c_ift_h", "c_ift_h_cal",
            ]),
            left_on="code_insee", right_on="insee_com", how="left",
        )
        .to_pandas()
    )

    # has_calendar_data : au moins une culture de la commune est dans le calendrier
    def _has_cal(row):
        dep = str(row.get("code_insee_dep", "") or "")
        for col in ("c_maj_cal", "c_ift_hbc_cal", "c_ift_h_cal"):
            val = row.get(col)
            if pd.notna(val) and (dep, val) in cal_pairs:
                return True
        return False

    df["has_calendar_data"] = df.apply(_has_cal, axis=1)
    df["nom_region"] = df["code_insee_reg"].map(REG_NOMS).fillna("Autre")
    return df


@st.cache_data(show_spinner="Chargement des indicateurs de risque…")
def load_risque(annee: int) -> pd.DataFrame | None:
    p = PARQUET / f"risque_journalier_{annee}.parquet"
    if not p.exists():
        return None
    df = pl.read_parquet(p).to_pandas()
    df["date"] = pd.to_datetime(df["date"])
    return df


def annees_disponibles() -> list[int]:
    return sorted(
        int(f.stem.split("_")[-1])
        for f in PARQUET.glob("risque_journalier_*.parquet")
        if f.stem.split("_")[-1].isdigit()
    )


def get_state(risque_0_4, ift_total, has_cal: bool) -> str | int:
    """Détermine l'état d'affichage d'une commune pour un jour donné."""
    if not has_cal:
        return "no_calendar"
    if pd.isna(risque_0_4):
        return "no_data"
    return int(risque_0_4)


# ── Application ──────────────────────────────────────────────

st.set_page_config(
    page_title="PestiExpo — Exposition aux pesticides",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🌿 PestiExpo — Exposition aux pesticides par commune")

communes = load_communes()
annees   = annees_disponibles()

# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")

    # Année et date
    risque    = None
    date_sel  = None
    annee_sel = None

    if annees:
        annee_sel = st.selectbox("Année", annees, index=len(annees) - 1)
        risque    = load_risque(annee_sel)

    if risque is not None:
        dates_dispo = sorted(risque["date"].dt.date.unique())
        date_sel    = st.date_input(
            "Date",
            value=dates_dispo[-1],
            min_value=dates_dispo[0],
            max_value=dates_dispo[-1],
        )
    else:
        st.info("Aucune donnée de risque disponible.\nLancez `etl/calcul_risque_journalier.py`.")

    st.divider()

    # Région
    regs_dispo   = sorted(communes["code_insee_reg"].dropna().unique())
    reg_options  = {"Toutes": None} | {f"{REG_NOMS.get(r, r)} ({r})": r for r in regs_dispo}
    reg_label    = st.selectbox("Région", list(reg_options.keys()))
    reg_sel      = reg_options[reg_label]

    # Département (filtré par région)
    base_deps = communes if reg_sel is None else communes[communes["code_insee_reg"] == reg_sel]
    deps_dispo = sorted(base_deps["code_insee_dep"].dropna().unique())
    dep_options = {"Tous": None} | {d: d for d in deps_dispo}
    dep_label   = st.selectbox("Département", list(dep_options.keys()))
    dep_sel     = dep_options[dep_label]

    st.divider()

    # Sélection de commune pour le détail
    st.subheader("Détail commune")
    view = communes.copy()
    if reg_sel:
        view = view[view["code_insee_reg"] == reg_sel]
    if dep_sel:
        view = view[view["code_insee_dep"] == dep_sel]

    commune_options = {"(aucune)": None} | {
        f"{r['nom_commune']} ({r['code_insee']})": r["code_insee"]
        for _, r in view.sort_values("nom_commune").iterrows()
    }
    commune_label = st.selectbox("Commune", list(commune_options.keys()))
    commune_sel   = commune_options[commune_label]

    st.divider()

    # Légende
    st.subheader("Légende")
    for key, (clr, lbl) in STATES.items():
        st.markdown(
            f'<span style="display:inline-block;width:12px;height:12px;background:{clr};'
            f'border-radius:50%;margin-right:6px;vertical-align:middle;border:1px solid #888">'
            f'</span>{lbl}',
            unsafe_allow_html=True,
        )


# ── Données carte ────────────────────────────────────────────
map_data = view.copy()

if risque is not None and date_sel is not None:
    rj = risque[risque["date"].dt.date == date_sel][
        ["insee_com", "risque_0_4", "ift_journalier_total",
         "interdiction_pulv", "pluie_limitante", "risque_dispersion"]
    ]
    map_data = map_data.merge(rj, left_on="code_insee", right_on="insee_com", how="left")
else:
    for col in ("risque_0_4", "ift_journalier_total", "interdiction_pulv",
                "pluie_limitante", "risque_dispersion"):
        map_data[col] = float("nan")

map_data["state"] = map_data.apply(
    lambda r: get_state(r["risque_0_4"], r.get("ift_journalier_total"), r["has_calendar_data"]),
    axis=1,
)
map_data["color"] = map_data["state"].map(lambda s: STATES[s][0])
map_data["label"] = map_data["state"].map(lambda s: STATES[s][1])
map_data["hover"] = map_data.apply(
    lambda r: (
        f"<b>{r['nom_commune']}</b> ({r['code_insee']})<br>"
        f"Dép. {r['code_insee_dep']} — {r.get('nom_region','')}<br>"
        + (f"Indicateur : {int(r['risque_0_4'])}<br>" if pd.notna(r.get('risque_0_4')) else "")
        + f"{r['label']}"
    ),
    axis=1,
)

# ── Métriques résumées ───────────────────────────────────────
col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
counts = map_data["state"].value_counts()
col_m1.metric("Hors calendrier", int(counts.get("no_calendar", 0)))
col_m2.metric("Données manquantes", int(counts.get("no_data", 0)))
col_m3.metric("Aucun traitement", int(counts.get(0, 0)))
col_m4.metric("Risque modéré (2-3)", int(counts.get(2, 0)) + int(counts.get(3, 0)))
col_m5.metric("Risque très élevé (4)", int(counts.get(4, 0)))

# ── Mise en page principale ──────────────────────────────────
col_carte, col_detail = st.columns([3, 1])

with col_carte:
    titre_date = str(date_sel) if date_sel else "— données non disponibles"
    st.subheader(f"Carte d'exposition — {titre_date}")

    fig_map = go.Figure()
    for state_key, (clr, lbl) in STATES.items():
        sub = map_data[map_data["state"] == state_key]
        if sub.empty:
            continue
        fig_map.add_trace(go.Scattermapbox(
            lat=sub["latitude"],
            lon=sub["longitude"],
            mode="markers",
            marker=dict(size=5, color=clr, opacity=0.85),
            text=sub["hover"],
            hoverinfo="text",
            name=lbl,
        ))

    # Surligner la commune sélectionnée
    if commune_sel:
        sel_row = map_data[map_data["code_insee"] == commune_sel]
        if not sel_row.empty:
            fig_map.add_trace(go.Scattermapbox(
                lat=sel_row["latitude"],
                lon=sel_row["longitude"],
                mode="markers",
                marker=dict(size=14, color="white", opacity=1,
                            symbol="circle"),
                text=sel_row["hover"],
                hoverinfo="text",
                name="Sélectionnée",
                showlegend=False,
            ))
            fig_map.add_trace(go.Scattermapbox(
                lat=sel_row["latitude"],
                lon=sel_row["longitude"],
                mode="markers",
                marker=dict(size=10, color=sel_row["color"].iloc[0], opacity=1),
                text=sel_row["hover"],
                hoverinfo="text",
                name="Sélectionnée",
                showlegend=False,
            ))

    center_lat = map_data["latitude"].mean() if not map_data.empty else 46.5
    center_lon = map_data["longitude"].mean() if not map_data.empty else 2.3
    zoom = 8 if dep_sel else (6 if reg_sel else 5)

    fig_map.update_layout(
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
    st.plotly_chart(fig_map, width='content')


with col_detail:
    if commune_sel is None:
        st.info("Sélectionnez une commune dans la barre latérale pour voir son évolution temporelle.")

    elif risque is None:
        info_row = communes[communes["code_insee"] == commune_sel].iloc[0]
        st.subheader(info_row["nom_commune"])
        st.caption(f"INSEE : {commune_sel} — Dép. {info_row['code_insee_dep']}")
        _cal = "✅ Cultures dans le calendrier" if info_row["has_calendar_data"] else "⚠️ Cultures hors calendrier"
        st.markdown(_cal)
        for col_r, col_c, titre in [
            ("c_maj",     "c_maj_cal",     "Culture principale"),
            ("c_ift_hbc", "c_ift_hbc_cal", "IFT fongicides / insecticides"),
            ("c_ift_h",   "c_ift_h_cal",   "IFT herbicides"),
        ]:
            raw = info_row.get(col_r, "—")
            cal = info_row.get(col_c)
            suffix = f" → **{cal}**" if pd.notna(cal) else " → *hors calendrier*"
            st.markdown(f"**{titre}** : {raw}{suffix}")
        st.warning("Lancez `etl/calcul_risque_journalier.py` pour voir l'évolution temporelle.")

    else:
        info_row = communes[communes["code_insee"] == commune_sel].iloc[0]
        st.subheader(info_row["nom_commune"])
        st.caption(f"INSEE : {commune_sel} — Dép. {info_row['code_insee_dep']}")

        # Cultures
        for col_r, col_c, titre in [
            ("c_maj",     "c_maj_cal",     "Culture principale"),
            ("c_ift_hbc", "c_ift_hbc_cal", "IFT fongi/insecti"),
            ("c_ift_h",   "c_ift_h_cal",   "IFT herbicides"),
        ]:
            raw = info_row.get(col_r, "—")
            cal = info_row.get(col_c)
            suffix = f" → **{cal}**" if pd.notna(cal) else " → *hors calendrier*"
            st.markdown(f"**{titre}** : {raw}{suffix}")

        st.divider()

        # Série temporelle
        ts = (
            risque[risque["insee_com"] == commune_sel]
            .sort_values("date")
            .copy()
        )
        ts["state"] = ts.apply(
            lambda r: get_state(
                r["risque_0_4"],
                r.get("ift_journalier_total"),
                bool(info_row["has_calendar_data"]),
            ),
            axis=1,
        )
        ts["color"]   = ts["state"].map(lambda s: STATES[s][0])
        ts["risque_y"] = ts["risque_0_4"].fillna(0)
        ts["hover_ts"] = ts.apply(
            lambda r: (
                f"{r['date'].strftime('%d/%m/%Y')}<br>"
                f"Indicateur : {int(r['risque_0_4']) if pd.notna(r['risque_0_4']) else '—'}<br>"
                + (f"IFT journalier : {r['ift_journalier_total']:.3f}<br>"
                   if pd.notna(r.get('ift_journalier_total')) else "")
                + (f"🚫 Vent fort" if r.get('interdiction_pulv') else "")
                + (f"🌧 Pluie" if r.get('pluie_limitante') else "")
            ),
            axis=1,
        )

        fig_ts = go.Figure(go.Bar(
            x=ts["date"],
            y=ts["risque_y"],
            marker_color=ts["color"],
            text=ts["hover_ts"],
            hovertemplate="%{text}<extra></extra>",
        ))
        fig_ts.update_layout(
            title="Évolution annuelle du risque",
            yaxis=dict(range=[0, 4.3], tickvals=[0, 1, 2, 3, 4],
                       ticktext=["0", "1", "2", "3", "4"]),
            xaxis=dict(tickformat="%b"),
            height=280,
            margin=dict(l=10, r=10, t=40, b=10),
            bargap=0,
        )
        st.plotly_chart(fig_ts, width='content')

        # Stats annuelles
        nb_jours = len(ts)
        if nb_jours > 0:
            st.markdown(f"**{nb_jours} jours** — "
                        f"Risque ≥3 : **{(ts['risque_y'] >= 3).sum()} j** — "
                        f"Risque 4 : **{(ts['risque_y'] == 4).sum()} j**")
