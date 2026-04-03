"""
PestiExpo Dashboard — Streamlit
Indicateur journalier d'exposition aux pesticides par commune.

Lancement local :
    uv run streamlit run dashboard/app.py
"""

import pandas as pd
import streamlit as st

from datetime import date as date_type

from config_app import STATES, REG_NOMS
from utils_app import (
    load_communes, load_geojson,
    load_risque, load_risque_previsions, annees_disponibles,
    build_map_data, make_fig_map, make_fig_ts,
)


# ── Application ──────────────────────────────────────────────

st.set_page_config(
    page_title="PestiExpo — Exposition aux pesticides",
    page_icon="🌿",
    layout="wide",
    initial_sidebar_state="expanded",
)
st.title("🌿 PestiExpo — Exposition aux pesticides par commune")

communes   = load_communes()
annees     = annees_disponibles()
geojson    = load_geojson()
previsions = load_risque_previsions()


# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")

    risque    = None
    date_sel  = None
    annee_sel = None

    if annees:
        annee_sel = st.selectbox("Année", annees, index=len(annees) - 1)
        risque    = load_risque(annee_sel)

    if risque is not None or previsions is not None:
        today      = date_type.today()
        dates_hist = set(risque["date"].dt.date.unique()) if risque is not None else set()
        dates_prev = set(previsions["date"].dt.date.unique()) if previsions is not None else set()
        all_dates  = sorted(dates_hist | dates_prev)
        default_date = today if today in all_dates else all_dates[-1]
        date_sel = st.date_input(
            "Date",
            value=default_date,
            min_value=all_dates[0],
            max_value=all_dates[-1],
        )
        if date_sel in dates_prev:
            st.info("⚡ Données prévisionnelles")
    else:
        st.info("Aucune donnée de risque disponible.\nLancez `etl/calcul_risque_journalier.py`.")

    st.divider()

    # Région
    regs_dispo  = sorted(communes["code_insee_reg"].dropna().unique())
    reg_options = {"Toutes": None} | {f"{REG_NOMS.get(r, r)} ({r})": r for r in regs_dispo}
    reg_label   = st.selectbox("Région", list(reg_options.keys()))
    reg_sel     = reg_options[reg_label]

    # Département
    base_deps   = communes if reg_sel is None else communes[communes["code_insee_reg"] == reg_sel]
    deps_dispo  = sorted(base_deps["code_insee_dep"].dropna().unique())
    dep_options = {"Tous": None} | {d: d for d in deps_dispo}
    dep_label   = st.selectbox("Département", list(dep_options.keys()))
    dep_sel     = dep_options[dep_label]

    st.divider()

    # Commune
    st.subheader("Détail commune")
    view: pd.DataFrame = communes.copy()
    if reg_sel:
        view = pd.DataFrame(view[view["code_insee_reg"] == reg_sel])
    if dep_sel:
        view = pd.DataFrame(view[view["code_insee_dep"] == dep_sel])

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
map_data, is_forecast_date = build_map_data(view, risque, previsions, date_sel)


# ── Métriques résumées ───────────────────────────────────────
col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
counts = map_data["state"].value_counts()
col_m1.metric("Hors calendrier",       int(counts.get("no_calendar", 0)))
col_m2.metric("Données manquantes",    int(counts.get("no_data", 0)))
col_m3.metric("Aucun traitement",      int(counts.get(0, 0)))
col_m4.metric("Risque modéré (2-3)",   int(counts.get(2, 0)) + int(counts.get(3, 0)))
col_m5.metric("Risque très élevé (4)", int(counts.get(4, 0)))


# ── Mise en page principale ──────────────────────────────────
col_carte, col_detail = st.columns([3, 1])

with col_carte:
    titre_date   = str(date_sel) if date_sel else "— données non disponibles"
    titre_suffix = " ⚡ prévision" if is_forecast_date else ""
    st.subheader(f"Carte d'exposition — {titre_date}{titre_suffix}")

    if not geojson:
        st.warning("Géométries non disponibles. Lancez `etl/etl_statique.py` pour les générer.")

    fig_map = make_fig_map(map_data, geojson, commune_sel, reg_sel, dep_sel)
    st.plotly_chart(fig_map, width='content')


with col_detail:
    if commune_sel is None:
        st.info("Sélectionnez une commune dans la barre latérale pour voir son évolution temporelle.")

    elif risque is None and previsions is None:
        info_row = communes[communes["code_insee"] == commune_sel].iloc[0]
        st.subheader(info_row["nom_commune"])
        st.caption(f"INSEE : {commune_sel} — Dép. {info_row['code_insee_dep']}")
        st.markdown("✅ Cultures dans le calendrier" if info_row["has_calendar_data"]
                    else "⚠️ Cultures hors calendrier")
        for col_r, col_c, titre in [
            ("c_maj",     "c_maj_cal",     "Culture principale"),
            ("c_ift_hbc", "c_ift_hbc_cal", "IFT hors biocontrôle"),
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

        for col_r, col_c, titre in [
            ("c_maj",     "c_maj_cal",     "Culture principale"),
            ("c_ift_hbc", "c_ift_hbc_cal", "IFT hors biocontrôle"),
            ("c_ift_h",   "c_ift_h_cal",   "IFT herbicides"),
        ]:
            raw = info_row.get(col_r, "—")
            cal = info_row.get(col_c)
            suffix = f" → **{cal}**" if pd.notna(cal) else " → *hors calendrier*"
            st.markdown(f"**{titre}** : {raw}{suffix}")

        st.divider()

        # Série temporelle
        parts = []
        if risque is not None:
            parts.append(risque[risque["insee_com"] == commune_sel].copy())
        if previsions is not None:
            parts.append(previsions[previsions["insee_com"] == commune_sel].copy())
        ts = pd.concat(parts).sort_values("date") if parts else pd.DataFrame()

        if not ts.empty:
            ts["has_calendar_data"] = bool(info_row["has_calendar_data"])
            fig_ts = make_fig_ts(ts)
            st.plotly_chart(fig_ts, width='content')

            ts_hist = ts[~ts["is_forecast"]]
            if not ts_hist.empty:
                risque_y = ts_hist["risque_0_4"].fillna(0)
                st.markdown(
                    f"**{len(ts_hist)} jours** — "
                    f"Risque ≥3 : **{(risque_y >= 3).sum()} j** — "
                    f"Risque 4 : **{(risque_y == 4).sum()} j**"
                )
        else:
            st.info("Pas de données pour cette commune.")
