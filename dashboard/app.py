"""
PestiExpo Dashboard — Streamlit
Indicateur journalier d'exposition aux pesticides par commune.

Lancement local :
    uv run streamlit run dashboard/app.py

Prérequis : l'API FastAPI doit être démarrée (cf. API_URL dans config_app.py).
    uv run uvicorn api.main:app --port 8000
"""

import pandas as pd
import streamlit as st
from streamlit_folium import st_folium

from datetime import date as date_type, timedelta

from config_app import STATES, REG_NOMS
from utils_app import (
    load_communes, load_geojson,
    annees_disponibles, load_previsions_dates,
    build_map_data, make_fig_map, make_map_folium, make_fig_ts,
    load_risque_serie, load_previsions_serie,
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
dates_prev = set(load_previsions_dates())


# ── Sidebar ──────────────────────────────────────────────────
with st.sidebar:
    st.header("Filtres")

    date_sel  = None
    annee_sel = None

    if annees:
        annee_sel = st.selectbox("Année", annees, index=len(annees) - 1)

        # Dates historiques = tous les jours de l'année jusqu'à aujourd'hui
        today     = date_type.today()
        year_end  = date_type(annee_sel, 12, 31)
        last_day  = min(today, year_end)
        nb_days   = (last_day - date_type(annee_sel, 1, 1)).days + 1
        dates_hist = {date_type(annee_sel, 1, 1) + timedelta(days=i) for i in range(nb_days)}

        all_dates  = sorted(dates_hist | dates_prev)
        if all_dates:
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
map_data, is_forecast_date = build_map_data(view, date_sel, dates_prev)


# ── Métriques résumées ───────────────────────────────────────
st.subheader(f"Vue d'ensemble sur la sélection — {len(map_data)} communes")

col_m1, col_m2, col_m3, col_m4, col_m5 = st.columns(5)
counts = map_data["state"].value_counts()
col_m1.metric("Hors calendrier",       int(counts.get("no_calendar", 0)))
col_m2.metric("Données manquantes",    int(counts.get("no_data", 0)))
col_m3.metric("Aucun traitement",      int(counts.get(0, 0)))
col_m4.metric("Risque modéré (2-3)",   int(counts.get(2, 0)) + int(counts.get(3, 0)))
col_m5.metric("Risque très élevé (4)", int(counts.get(4, 0)))


# ── Mise en page principale ──────────────────────────────────
col_carte, col_empty, col_detail = st.columns([1, 0.25, 1])

with col_carte:
    titre_date   = str(date_sel) if date_sel else "— données non disponibles"
    titre_suffix = " ⚡ prévision" if is_forecast_date else ""
    st.subheader(f"Carte d'exposition — {titre_date}{titre_suffix}")

    if not geojson:
        st.warning("Géométries non disponibles. Lancez `etl/etl_statique.py` pour les générer.")

    
    if geojson:
        fig_map = make_fig_map(map_data, geojson, commune_sel, reg_sel, dep_sel)
        st.plotly_chart(fig_map, width='content')

    else:
        st_folium(make_map_folium(map_data, geojson), width=1200, height=600)

with col_detail:
    if commune_sel is None:
        st.info("Sélectionnez une commune dans la barre latérale pour voir son évolution temporelle.")

    else:
        info_row = communes[communes["code_insee"] == commune_sel].iloc[0]
        st.subheader(info_row["nom_commune"])
        st.caption(f"INSEE : {commune_sel} — Dép. {info_row['code_insee_dep']}")


        # Valeurs du jour demandé pour la commune
        selected_day = map_data[map_data["code_insee"] == commune_sel]
        if not selected_day.empty:
            day = selected_day.iloc[0]
            st.markdown(
                f"**Date sélectionnée** : {date_sel} "
                f"{'⚡ prévision' if is_forecast_date else ''}"
            )
            st.markdown(
                f"- **Risque 0-4** : "
                f"{int(day['risque_0_4']) if pd.notna(day.get('risque_0_4')) else '—'}"
            )
            st.markdown(
                f"- **Risque agronomique** : "
                f"{day['ift_journalier_total']:.3f}" if pd.notna(day.get('ift_journalier_total')) else "- **IFT journalier total** : —"
            )
            st.markdown(
                f"- **Vent fort** : {'oui' if day.get('interdiction_pulv') else 'non'}"
            )
            st.markdown(
                f"- **Pluie limitante** : {'oui' if day.get('pluie_limitante') else 'non'}"
            )
            st.markdown(
                f"- **Risque dispersion** : "
                f"{day['risque_dispersion'] if pd.notna(day.get('risque_dispersion')) else '—'}"
            )
        else:
            st.info("Aucune donnée disponible pour la commune à la date sélectionnée.")


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

        if annee_sel is None:
            st.warning("Sélectionnez une année pour voir l'évolution temporelle.")
        else:
            ts_hist = load_risque_serie(commune_sel, annee_sel)
            ts_prev = load_previsions_serie(commune_sel)

            parts = [df for df in [ts_hist, ts_prev] if not df.empty]
            ts = pd.concat(parts).sort_values("date") if parts else pd.DataFrame()

            if not ts.empty:
                ts["has_calendar_data"] = bool(info_row["has_calendar_data"])
                fig_ts = make_fig_ts(ts)
                st.plotly_chart(fig_ts, width='content')

                if not ts_hist.empty:
                    risque_y = ts_hist["risque_0_4"].fillna(0)
                    st.markdown(
                        f"**{len(ts_hist)} jours** — "
                        f"Risque ≥3 : **{(risque_y >= 3).sum()} j** — "
                        f"Risque 4 : **{(risque_y == 4).sum()} j**"
                    )
            else:
                st.info("Pas de données pour cette commune.")
