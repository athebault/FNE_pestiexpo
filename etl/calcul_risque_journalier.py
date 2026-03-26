"""
Calcul indicateur de risque journalier pesticides par commune.
Basé sur : cultures majoritaires × calendrier épandage × météo journalière.

Logique :
- IFT journalier culture = IFT_annuel / nb_periodes_calendrier (culture × département)
- Risque commune = somme des IFT journaliers des 3 cultures × facteur météo
- Indicateur final normalisé 0-4 par quartiles sur l'année
"""

import polars as pl
import duckdb
import logging
from datetime import date, timedelta
from config import DUCKDB_PATH, PARQUET_DIR, VENT_MAX, VENT_DISPERSION, PLUIE_SEUIL, METEO_ENABLED

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

FACTEUR_DISPERSION = 1.5

# ============================================================
# TABLE DE CORRESPONDANCE  cultures IFT (ADONIS) → calendrier
# ============================================================
CULTURE_MAPPING: dict[str, str] = {
    # Blé
    "Blé tendre":                                   "Blé tendre",
    "Blé dur":                                      "Blé dur",
    # Orge
    "Orge d'hiver":                                 "Orge d'hiver",
    "Orge de printemps":                            "Orge de printemps",
    # Céréales d'hiver secondaires
    "Triticale":                                    "Céréales d'hiver",
    "Épeautre":                                     "Céréales d'hiver",
    "Seigle d'hiver":                               "Céréales d'hiver",
    # Céréales de printemps / indéterminées
    "Avoine":                                       "Céréales",
    "Seigle de printemps":                          "Céréales",
    "Mélange de céréales":                          "Céréales",
    # Maïs
    "Maïs":                                         "Maïs",
    "Maïs ensilage":                                "Maïs",
    "Maïs doux":                                    "Maïs",
    # Oléagineux
    "Colza":                                        "Colza",
    "Tournesol":                                    "Tournesol",
    "Lin fibres":                                   "Lin printemps",
    "Lin oléagineux":                               "Lin printemps",
    # Cultures diverses
    "Sorgho":                                       "Sorgho",
    "Millet":                                       "Millet",
    "Sarrasin":                                     "Céréales",
    "Riz":                                          "Céréales",
    # Betteraves
    "Betterave non fourragère / Bette":             "Betterave",
    # Pommes de terre
    "Pomme de terre de consommation":               "Pomme de terre de consommation",
    "Pomme de terre féculière":                     "Pomme de terre de consommation",
    "Pomme de terre primeur":                       "Pomme de terre de consommation",
    # Légumineuses
    "Féverole":                                     "Féverole",
    "Pois protéagineux":                            "Pois protéagineux",
    "Petits pois":                                  "Pois de printemps",
    "Lentille cultivée (non fourragère)":           "Pois de printemps",
    "Pois chiche":                                  "Pois de printemps",
    "Mélange de légumineuses":                      "Pois protéagineux",
    "Mélange de légumineuses fourragères":          "Luzerne",
    "Mélange de légumineuses fourragères (entre elles)": "Luzerne",
    "Mélange de protéagineux":                      "Pois protéagineux",
    "Autre mélange de plantes fixant l'azote":      "Luzerne",
    # Soja → légumineuse la plus proche agronomiquement
    "Soja":                                         "Féverole d'hiver",
    # Lin
    "Lin d'hiver":                                  "Lin d'hiver",
    # Luzerne
    "Luzerne déshydratée":                          "Luzerne",
    "Autre luzerne":                                "Luzerne",
    "Autre sainfoin":                               "Luzerne",
    "Autre trèfle":                                 "Luzerne",
    # Prairies et surfaces enherbées
    "Prairie permanente":                           "Prairies",
    "Prairie en rotation longue (6 ans ou plus)":  "Prairies",
    "Autre prairie temporaire de 5 ans ou moins":  "Prairies",
    "Ray-grass de 5 ans ou moins":                 "Prairies",
    "Surface pastorale (SPH)":                     "Prairies",
    "Surface pastorale (SPL)":                     "Prairies",
    "Bois pâturé":                                  "Prairies",
    # Vigne
    "Vigne":                                        "Vigne",
    "Vigne : raisins de cuve":                      "Vigne",
    # Arboriculture
    "Vergers":                                      "Arboriculture",
    "Cerise bigarreau pour transformation":         "Arboriculture",
    "Prune d'Ente pour transformation":             "Arboriculture",
    "Noisette":                                     "Arboriculture",
    "Noix":                                         "Arboriculture",
    "Châtaigne":                                    "Arboriculture",
    "Châtaigneraie":                                "Arboriculture",
    "Agrume":                                       "Arboriculture",
    "Oliveraie":                                    "Arboriculture",
    "Petit fruit rouge":                            "Arboriculture",
    "Fraise":                                       "Arboriculture",
}


def normaliser_culture(expr: pl.Expr) -> pl.Expr:
    """Applique CULTURE_MAPPING sur une expression Polars, retourne None si absent."""
    return expr.replace_strict(
        old=list(CULTURE_MAPPING.keys()),
        new=list(CULTURE_MAPPING.values()),
        default=None,
    )


# ============================================================
# 1. CHARGEMENT
# ============================================================
def load_data(annee: int) -> tuple:

    con = duckdb.connect(str(DUCKDB_PATH))

    ift_raw = con.execute("""
        SELECT
            insee_com,
            code_insee_dep,
            c_maj,     ift_t_hbc  AS ift_maj,
            c_ift_hbc, ift_hh_hbc AS ift_hbc,
            c_ift_h,   ift_h      AS ift_h
        FROM ift_communes_enrichi
        WHERE ift_t IS NOT NULL
    """).pl()

    # Normalisation des noms de culture vers les noms du calendrier
    ift = ift_raw.with_columns([
        normaliser_culture(pl.col("c_maj")).alias("c_maj_cal"),
        normaliser_culture(pl.col("c_ift_hbc")).alias("c_ift_hbc_cal"),
        normaliser_culture(pl.col("c_ift_h")).alias("c_ift_h_cal"),
    ])

    # Calendrier avec nb_periodes par type × culture × département
    # Les dates sont normalisées vers l'année cible (le calendrier peut être stocké
    # pour n'importe quelle année dans le parquet — seul le mois/jour compte).
    cal = con.execute(f"""
        SELECT
            departement_code,
            culture,
            make_date({annee}, month(Debut_de_periode), day(Debut_de_periode)) AS Debut_de_periode,
            make_date({annee}, month(Fin_de_periode),   day(Fin_de_periode))   AS Fin_de_periode,
            Herbicides,
            Fongicides,
            Insecticides,
            -- Nb périodes herbicides pour cette culture × département
            COUNT(*) FILTER (WHERE Herbicides = true)
                OVER (PARTITION BY departement_code, culture) AS nb_periodes_herbicides,
            -- Nb périodes fongicides+insecticides
            COUNT(*) FILTER (WHERE Fongicides = true OR Insecticides = true)
                OVER (PARTITION BY departement_code, culture) AS nb_periodes_fongi_insecti,
            -- Nb périodes total
            COUNT(*) OVER (PARTITION BY departement_code, culture) AS nb_periodes_total
        FROM calendrier_epandage
    """).pl()

    meteo = None
    if not METEO_ENABLED:
        logger.info("METEO_ENABLED=False : chargement météo ignoré, facteur_meteo=1.0 utilisé par défaut")
    else:
        try:
            meteo = pl.read_parquet(
                PARQUET_DIR.parent / f"meteo/historique/annee={annee}/meteo.parquet"
            ).select(["code_insee", "time", "wind_speed_10m_max", "precipitation_sum"])
        except FileNotFoundError:
            logger.warning("Fichier météo introuvable pour l'année %s -> mode sans météo", annee)
            meteo = None

    con.close()
    return ift, cal, meteo


# ============================================================
# 2. IFT JOURNALIER PAR COMMUNE
# ============================================================
def compute_ift_journalier(
    ift: pl.DataFrame,
    cal: pl.DataFrame,
    date_cible: date
) -> pl.DataFrame:
    """
    Pour chaque commune et chaque culture (c_maj, c_ift_hbc, c_ift_h) :
    - Vérifie si la date tombe dans une période active
    - IFT journalier = IFT_annuel / nb_periodes_calendrier si actif, sinon 0
    - Somme les 3 IFT journaliers par commune
    """

    resultats = []
    cal_for_join = cal.with_columns([pl.col("departement_code").cast(pl.Utf8)])
    cal_active = cal_for_join.filter(
        (pl.col("Debut_de_periode") <= date_cible) &
        (pl.col("Fin_de_periode")   >= date_cible)
    )

    # (col_culture_norm, col_ift, filtre_type, col_nb_periodes)
    configs = [
        ("c_maj_cal",     "ift_maj",  None,                                          "nb_periodes_total"),
        ("c_ift_hbc_cal", "ift_hbc",  pl.col("Fongicides") | pl.col("Insecticides"), "nb_periodes_fongi_insecti"),
        ("c_ift_h_cal",   "ift_h",    pl.col("Herbicides"),                          "nb_periodes_herbicides"),
    ]

    for col_culture_cal, col_ift, filtre_type, col_nb in configs:
        # Périodes actives filtrées par type de traitement
        if filtre_type is not None:
            cal_active_type = cal_active.filter(filtre_type)
        else:
            cal_active_type = cal_active

        joined = (
            ift.select(["insee_com", "code_insee_dep", col_culture_cal, col_ift])
            .rename({col_culture_cal: "culture", col_ift: "ift_annuel"})
            # Jointure sur nb_periodes (toutes périodes, pas seulement actives)
            .join(
                cal_for_join.select(["departement_code", "culture", col_nb])
                   .unique(["departement_code", "culture"]),
                left_on=["code_insee_dep", "culture"],
                right_on=["departement_code", "culture"],
                how="left"
            )
            # Jointure sur périodes actives (pour savoir si on est dans une période)
            .join(
                cal_active_type.select(["departement_code", "culture"])
                          .with_columns(pl.lit(True).alias("periode_active")),
                left_on=["code_insee_dep", "culture"],
                right_on=["departement_code", "culture"],
                how="left"
            )
            .with_columns([
                # IFT journalier = IFT / nb_periodes si période active
                pl.when(
                    pl.col("periode_active").is_not_null() &
                    pl.col(col_nb).is_not_null()
                )
                .then(pl.col("ift_annuel") / pl.col(col_nb))
                .otherwise(0.0)
                .alias("ift_journalier")
            ])
            .select(["insee_com", "ift_journalier"])
        )
        resultats.append(joined)

    # Somme des 3 IFT journaliers par commune
    df = (
        pl.concat(resultats)
        .group_by("insee_com")
        .agg(pl.col("ift_journalier").sum().alias("ift_journalier_total"))
    )

    return df


# ============================================================
# 3. FACTEUR MÉTÉO
# ============================================================
def compute_facteur_meteo(meteo_jour: pl.DataFrame) -> pl.DataFrame:
    return meteo_jour.with_columns([
        pl.when(pl.col("wind_speed_10m_max") >= VENT_MAX)
          .then(0.0)
        .when(pl.col("precipitation_sum") >= PLUIE_SEUIL)
          .then(0.0)
        .when(
            (pl.col("wind_speed_10m_max") >= VENT_DISPERSION) &
            (pl.col("wind_speed_10m_max") <  VENT_MAX)
        )
          .then(FACTEUR_DISPERSION)
        .otherwise(1.0)
        .alias("facteur_meteo"),

        (pl.col("wind_speed_10m_max") >= VENT_MAX).alias("interdiction_pulv"),
        (pl.col("precipitation_sum")  >= PLUIE_SEUIL).alias("pluie_limitante"),
        (
            (pl.col("wind_speed_10m_max") >= VENT_DISPERSION) &
            (pl.col("wind_speed_10m_max") <  VENT_MAX)
        ).alias("risque_dispersion"),
    ])


# ============================================================
# 4. NORMALISATION 0-4 PAR QUARTILES
# ============================================================
def normalize_0_4(df: pl.DataFrame, col: str = "risque_brut") -> pl.DataFrame:
    """
    Normalise l'indicateur entre 0 et 4 par quartiles sur l'ensemble
    des valeurs (toutes communes × tous jours).
    0   = pas de risque (risque_brut == 0)
    1   = risque faible   (Q1)
    2   = risque modéré   (Q2)
    3   = risque élevé    (Q3)
    4   = risque très élevé (au-delà de Q3)
    """
    # Calcul des quartiles sur les valeurs strictement positives
    valeurs_positives = df.filter(pl.col(col) > 0)[col]
    if valeurs_positives.is_empty():
        logger.info("Aucune valeur de risque brut > 0, normalisation en 0 partout")
        return df.with_columns(pl.lit(0).alias("risque_0_4"))

    q1 = valeurs_positives.quantile(0.25)
    q2 = valeurs_positives.quantile(0.50)
    q3 = valeurs_positives.quantile(0.75)

    logger.info(f"Quartiles risque brut — Q1: {q1:.4f} | Q2: {q2:.4f} | Q3: {q3:.4f}")

    return df.with_columns(
        pl.when(pl.col(col) == 0)
          .then(0)
        .when(pl.col(col) <= q1)
          .then(1)
        .when(pl.col(col) <= q2)
          .then(2)
        .when(pl.col(col) <= q3)
          .then(3)
        .otherwise(4)
        .alias("risque_0_4")
    )


# ============================================================
# 5. PIPELINE PRINCIPAL
# ============================================================
def compute_risque_journalier(annee: int) -> pl.DataFrame:

    ift, cal, meteo = load_data(annee)

    nb_jours = 366 if annee % 4 == 0 else 365
    dates = [date(annee, 1, 1) + timedelta(days=d) for d in range(nb_jours)]

    resultats = []
    for i, d in enumerate(dates):
        if i % 30 == 0:
            logger.info(f"  Jour {i+1}/{nb_jours} — {d}")

        ift_jour = compute_ift_journalier(ift, cal, d)

        if METEO_ENABLED and meteo is not None:
            meteo_jour = (
                meteo.filter(pl.col("time") == d)
                .pipe(compute_facteur_meteo)
            )
        else:
            meteo_jour = (
                ift_jour.select(pl.col("insee_com").alias("code_insee"))
                .with_columns([
                    pl.lit(1.0).alias("facteur_meteo"),
                    pl.lit(False).alias("interdiction_pulv"),
                    pl.lit(False).alias("pluie_limitante"),
                    pl.lit(False).alias("risque_dispersion"),
                    pl.lit(None).cast(pl.Float64).alias("wind_speed_10m_max"),
                    pl.lit(None).cast(pl.Float64).alias("precipitation_sum"),
                ])
            )

        risque_jour = (
            ift_jour.join(
                meteo_jour.select([
                    "code_insee", "facteur_meteo",
                    "interdiction_pulv", "pluie_limitante",
                    "risque_dispersion", "wind_speed_10m_max", "precipitation_sum"
                ]),
                left_on="insee_com",
                right_on="code_insee",
                how="left"
            )
            .with_columns([
                (pl.col("ift_journalier_total") * pl.col("facteur_meteo"))
                  .alias("risque_brut"),
                pl.lit(d).alias("date"),
            ])
        )
        resultats.append(risque_jour)

    df_final = pl.concat(resultats)

    # Normalisation 0-4
    df_final = normalize_0_4(df_final, col="risque_brut")

    return df_final.sort(["insee_com", "date"])


# ============================================================
# 6. SAUVEGARDE
# ============================================================
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--annee", type=int, default=2025)
    args = parser.parse_args()

    logger.info(f"Calcul risque journalier {args.annee}...")
    df = compute_risque_journalier(args.annee)

    output = PARQUET_DIR / f"risque_journalier_{args.annee}.parquet"
    df.write_parquet(output)
    logger.info(f"✓ {output} — {df.shape[0]:,} lignes ({df['date'].n_unique()} jours × {df['insee_com'].n_unique()} communes)")
    print(df.head(10))