"""
Calcul indicateur de risque journalier pesticides par commune.
Basé sur : cultures majoritaires × calendrier épandage × météo journalière.

Logique :
- IFT journalier culture = IFT_annuel / nb_periodes_calendrier (culture × département)
- Risque commune = somme des IFT journaliers des 3 cultures × indicateur météo
- Indicateur final normalisé 0-4 par quartiles sur l'année
"""

import polars as pl
import logging
from datetime import date, timedelta
from etl_config import *
from utils import get_duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


logger.info("Valeurs seuils météo — Vent max : %d m/s | Dispersion modérée : %d-%d m/s | Pluie : > %.1f mm",
            VENT_MAX, VENT_DISPERSION_MAX, VENT_DISPERSION_MIN, PLUIE_SEUIL)


def normaliser_culture(expr: pl.Expr) -> pl.Expr:
    """Applique CULTURE_MAPPING sur une expression Polars, retourne None si absent."""
    return expr.replace_strict(
        old=list(CULTURE_MAPPING.keys()),
        new=list(CULTURE_MAPPING.values()),
        default=None,
    )


# ============================================================
# 1. CHARGEMENT depuis la base de données DuckDB
# ============================================================
def load_data(annee: int, region: str | None = None) -> tuple:

    if region is not None and region not in code_region_rpg:
        regions_valides = ", ".join(sorted(code_region_rpg.keys()))
        raise ValueError(f"Région inconnue : '{region}'. Régions valides : {regions_valides}")

    con = get_duckdb()

    region_filter = ""
    if region is not None:
        code_reg = code_region_rpg[region]
        region_filter = f"AND code_insee_reg = '{code_reg}'"
        logger.info(f"Filtre région : {region} (code {code_reg})")

    ift_raw = con.execute(f"""
        SELECT
            insee_com,
            code_insee_dep,
            code_insee_reg,
            c_maj,     ift_t_hbc  AS ift_maj_hbc,
            c_ift_hbc, ift_hh_hbc AS ift_hh_hbc,
            c_ift_h,   ift_h      AS ift_h
        FROM ift_communes_enrichi
        WHERE ift_t IS NOT NULL {region_filter}
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
        # Pas de prise en compte de la météo : indicateur_meteo = 1 (conditions normales, pas d'amplification du risque)
        logger.info("METEO_ENABLED=False : chargement météo ignoré, indicateur_meteo=1 utilisé par défaut")
    else:
        try:
            # Récupération des données météo permettant le calcul de l'indicateur de dispersion (vent + pluie)
            meteo = pl.read_parquet(
                METEO_DIR / f"historique/{annee}/meteo.parquet"
            ).select(["code_insee", "time", "wind_speed_10m_mean", "precipitation_sum"])
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
    - Agrège les contributions en évitant le double-comptage :
        c_ift_hbc et c_ift_h ne sont ajoutés que s'ils diffèrent de c_maj
    """

    cal_for_join = cal.with_columns([pl.col("departement_code").cast(pl.Utf8)])

    # La date cible est elle sur une période active (i.e de traitement) pour cette culture × département ?
    cal_active = cal_for_join.filter(
        (pl.col("Debut_de_periode") <= date_cible) &
        (pl.col("Fin_de_periode")   >= date_cible)
    )

    # Correspondance département → région (déduite des données IFT)
    dept_to_region = ift.select(["code_insee_dep", "code_insee_reg"]).unique()

    # Calendrier enrichi avec code région (pour le fallback régional)
    cal_with_region = cal_for_join.join(
        dept_to_region, left_on="departement_code", right_on="code_insee_dep", how="inner"
    )

    # (col_culture_norm, col_ift, filtre_type, col_nb_periodes, suffixe_résultat)
    configs = [
        ("c_maj_cal",     "ift_maj_hbc", None,                                         "nb_periodes_total",         "maj_hbc"),
        ("c_ift_hbc_cal", "ift_hh_hbc", pl.col("Fongicides") | pl.col("Insecticides"), "nb_periodes_fongi_insecti", "hh_hbc"),
        ("c_ift_h_cal",   "ift_h",      pl.col("Herbicides"),                          "nb_periodes_herbicides",    "h"),
    ]

    result_dfs = []
    for col_culture_cal, col_ift, filtre_type, col_nb, suffix in configs:
        cal_active_type = cal_active.filter(filtre_type) if filtre_type is not None else cal_active

        # Fallback régional : nb_periodes moyen des départements de la région
        cal_region_nb = (
            cal_with_region.select(["code_insee_reg", "departement_code", "culture", col_nb])
            .unique(["code_insee_reg", "departement_code", "culture"])
            .group_by(["code_insee_reg", "culture"])
            .agg(pl.col(col_nb).mean().alias(f"{col_nb}_reg"))
        )

        # Fallback régional : période active si elle l'est pour au moins un département de la région
        cal_active_region = (
            cal_active_type.select(["departement_code", "culture"])
            .unique()
            .join(dept_to_region, left_on="departement_code", right_on="code_insee_dep", how="inner")
            .select(["code_insee_reg", "culture"])
            .unique()
            .with_columns(pl.lit(True).alias("periode_active_reg"))
        )

        joined = (
            ift.select(["insee_com", "code_insee_dep", "code_insee_reg", col_culture_cal, col_ift])
            .rename({col_culture_cal: "culture", col_ift: "ift_annuel"})
            .join(
                cal_for_join.select(["departement_code", "culture", col_nb])
                   .unique(["departement_code", "culture"]),
                left_on=["code_insee_dep", "culture"],
                right_on=["departement_code", "culture"],
                how="left"
            )
            .join(
                cal_active_type.select(["departement_code", "culture"])
                          .unique(["departement_code", "culture"])
                          .with_columns(pl.lit(True).alias("periode_active")),
                left_on=["code_insee_dep", "culture"],
                right_on=["departement_code", "culture"],
                how="left"
            )
            # Fallback sur le calendrier régional si le département est absent
            .join(cal_region_nb, on=["code_insee_reg", "culture"], how="left")
            .join(cal_active_region, on=["code_insee_reg", "culture"], how="left")
            .with_columns([
                pl.when(pl.col(col_nb).is_not_null())
                  .then(pl.col(col_nb))
                  .otherwise(pl.col(f"{col_nb}_reg"))
                  .alias(col_nb),
                pl.when(pl.col("periode_active").is_not_null())
                  .then(pl.col("periode_active"))
                  .otherwise(pl.col("periode_active_reg"))
                  .alias("periode_active"),
            ])
            .with_columns(
                pl.when((pl.col(col_nb).is_null()) & (pl.col("ift_annuel") == 0))
                    .then(pl.lit(0, dtype=pl.Float64))  # Pas de traitement donc normal de ne pas apparaître dans le calendrier → contribution 0
                .when((pl.col("ift_annuel").is_null()))
                    .then(pl.lit(0, dtype=pl.Float64))  # Pas de parcelle agricoles → contribution 0
                .when((pl.col(col_nb).is_null()) & (pl.col("ift_annuel") != 0))
                    .then(pl.lit(None, dtype=pl.Float64)) # IFT non nul mais pas de période dans le calendrier → incohérence dans les données source, contribution indéterminée → NaN
                .when(pl.col("periode_active").is_not_null())
                    .then(pl.col("ift_annuel") / pl.col(col_nb))
                .otherwise(pl.lit(0.0))
                .alias(f"ift_j_{suffix}")
            )
            .select(["insee_com", f"ift_j_{suffix}"])
        )

        result_dfs.append(joined)

    # Jointure côte à côte des 3 contributions
    df = result_dfs[0]
    for rdf in result_dfs[1:]:
        df = df.join(rdf, on="insee_com", how="left")

    # Flags de déduplication : c_ift_hh_hbc / c_ift_h inclus seulement si culture différente de c_maj
    # Comparaison sur les noms calendrier (_cal) : deux cultures ADONIS qui mappent vers la même
    # entrée calendrier sont considérées identiques (même périodes, même référence IFT).
    dedup = ift.select([
        "insee_com",
        (pl.col("c_ift_hbc_cal") != pl.col("c_maj_cal")).fill_null(True).alias("hbc_different"),
        (pl.col("c_ift_h_cal")   != pl.col("c_maj_cal")).fill_null(True).alias("h_different"),
    ])
    df = df.join(dedup, on="insee_com", how="left")

    # Agrégation avec propagation des nulls :
    # - None si aucune culture contributrice n'a de données calendrier
    # - sinon somme des contributions (0 si culture exclue par dédup ou hors période)
    # has_any_data vérifie uniquement les cultures qui contribuent effectivement
    has_any_data = (
        pl.col("ift_j_maj_hbc").is_not_null()
        | (pl.col("hbc_different") & pl.col("ift_j_hh_hbc").is_not_null())
        | (pl.col("h_different")   & pl.col("ift_j_h").is_not_null())
    )

    df = df.with_columns(
        pl.when(has_any_data)
        .then(
            pl.col("ift_j_maj_hbc").fill_null(0.0)
            + pl.when(pl.col("hbc_different")).then(pl.col("ift_j_hh_hbc").fill_null(0.0)).otherwise(0.0)
            + pl.when(pl.col("h_different")).then(pl.col("ift_j_h").fill_null(0.0)).otherwise(0.0)
        )
        .otherwise(pl.lit(None, dtype=pl.Float64))
        .alias("ift_journalier_total")
    ).select(["insee_com", "ift_journalier_total"])

    return df


# ============================================================
# 3. INDICATEUR MÉTÉO  (0 – 3)
# ============================================================
def compute_indicateur_meteo(meteo_jour: pl.DataFrame) -> pl.DataFrame:
    """
    Indicateur de conditions météo favorables à la dispersion des pesticides.

    0 - Pas de dispersion : pluie (> 0 mm)
        La pluie empêche l'épandage et lave les dépôts 
    0 - Interdiction de pulvérisation : vent ≥ VENT_MAX (19 m/s)
    0 - Forte dispersion : VENT_DISPERSION_MIN (11) ≤ vent < VENT_MAX (19 m/s)
        Même sans pluie, le vent fort rend la pulvérisation dangereuse 
        → l'agriculture ne va pas pulvériser, risque nul
        → risque nul car traitement fortement déconseillé
    1 - Dispersion modérée : VENT_DISPERSION_MAX (5) ≤ vent < VENT_DISPERSION_MIN (11 m/s)
    2 - Conditions calmes, faible risque de dérive : vent < VENT_DISPERSION_MAX (5 m/s)
        → plus haut risque d'exposition pour les habitants à proximité

    """
    pluie = pl.col("precipitation_sum") > PLUIE_SEUIL   
    vent  = pl.col("wind_speed_10m_mean")

    return meteo_jour.with_columns([
        pl.when(pluie | (vent >= VENT_DISPERSION_MIN))
          .then(pl.lit(0))
        .when(vent <= VENT_DISPERSION_MAX)
          .then(pl.lit(2))
        .otherwise(pl.lit(1))
        .cast(pl.Int32)
        .alias("indicateur_meteo"),

        (vent >= VENT_MAX).alias("interdiction_pulv"),
        (vent >= VENT_DISPERSION_MIN).alias("traitement_deconseille"),
        (pl.col("precipitation_sum") > PLUIE_SEUIL).alias("pluie_limitante"),
        (vent < VENT_DISPERSION_MIN).alias("risque_dispersion"),
    ])


# ============================================================
# 4. NORMALISATION 0-4 PAR valeurs seuils
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
    # Obtention des valeurs seuils
    q1, q2, q3 = VALEURS_SEUIL  # Seuils fixes pour les niveaux de risque
    logger.info(f"Valeurs seuils risque brut — 1: {q1:.4f} | 2: {q2:.4f} | 3: {q3:.4f}")
    
    # Verification que df n'est pas vide
    if df.is_empty():
        logger.warning("DataFrame vide : aucune donnée pour normalisation. Ajout de la colonne risque_0_4 avec des valeurs nulles.")
        return df.with_columns(pl.lit(None).cast(pl.Int32).alias("risque_0_4")) 
    else:
        return df.with_columns(
            pl.when(pl.col(col).is_null())
            .then(pl.lit(None))
            .when(pl.col(col) == 0)
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
def compute_risque_journalier(annee: int, region: str | None = None) -> pl.DataFrame:

    ift, cal, meteo = load_data(annee, region)

    nb_jours = 366 if annee % 4 == 0 else 365
    dates = [date(annee, 1, 1) + timedelta(days=d) for d in range(nb_jours)]

    resultats = []
    for i, d in enumerate(dates):
        if i % 30 == 0:
            logger.info(f"  Jour {i+1}/{nb_jours} — {d}")

        ift_jour = compute_ift_journalier(ift, cal, d)

        meteo_jour_raw = meteo.filter(pl.col("time") == d) if (METEO_ENABLED and meteo is not None) else None

        if meteo_jour_raw is not None and not meteo_jour_raw.is_empty():
            meteo_jour = meteo_jour_raw.pipe(compute_indicateur_meteo)
        else:
            # Pas de météo ou date hors couverture ERA5 (lag ~5-7j) → indicateur neutre
            if METEO_ENABLED and meteo is not None and meteo_jour_raw is not None:
                logger.debug(f"  {d} : hors couverture ERA5, indicateur_meteo=1 (neutre)")
            meteo_jour = (
                ift_jour.select(pl.col("insee_com").alias("code_insee"))
                .with_columns([
                    pl.lit(1).cast(pl.Int32).alias("indicateur_meteo"),
                    pl.lit(False).alias("interdiction_pulv"),
                    pl.lit(False).alias("traitement_deconseille"),
                    pl.lit(False).alias("pluie_limitante"),
                    pl.lit(True).alias("risque_dispersion"),
                    pl.lit(None).cast(pl.Float64).alias("wind_speed_10m_mean"),
                    pl.lit(None).cast(pl.Float64).alias("precipitation_sum"),
                ])
            )

        risque_jour = (
            ift_jour.join(
                meteo_jour.select([
                    "code_insee", "indicateur_meteo",
                    "interdiction_pulv", "traitement_deconseille", "pluie_limitante",
                    "risque_dispersion", "wind_speed_10m_mean", "precipitation_sum"
                ]),
                left_on="insee_com",
                right_on="code_insee",
                how="left"
            )
            .with_columns([
                (pl.col("ift_journalier_total") * pl.col("indicateur_meteo"))
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
# 6. RISQUE PRÉVISIONNEL (J+7)
# ============================================================
def compute_risque_previsions(annee: int, region: str | None = None) -> pl.DataFrame | None:
    """
    Calcule l'indicateur de risque pour les jours de prévision météo disponibles (max 7 jours).
    - IFT et calendrier : identiques au calcul historique
    - Météo : chargée depuis data/meteo/previsions/meteo_previsions.parquet
    - Normalisation 0-4 : réutilise les quartiles du risque annuel historique (même échelle)
    """
    previsions_path = METEO_DIR / "previsions/meteo_previsions.parquet"
    if not previsions_path.exists():
        logger.warning("Prévisions météo non disponibles : %s", previsions_path)
        return None

    ift, cal, _ = load_data(annee, region)

    meteo_prev = pl.read_parquet(previsions_path).select(
        ["code_insee", "time", "wind_speed_10m_mean", "precipitation_sum"]
    )
    dates_prev = sorted(meteo_prev["time"].unique().to_list())
    logger.info(f"Risque prévisions — {len(dates_prev)} jours : {dates_prev[0]} → {dates_prev[-1]}")

    resultats = []
    for d in dates_prev:
        ift_jour = compute_ift_journalier(ift, cal, d)
        meteo_jour = (
            meteo_prev.filter(pl.col("time") == d)
            .pipe(compute_indicateur_meteo)
        )
        risque_jour = (
            ift_jour.join(
                meteo_jour.select([
                    "code_insee", "indicateur_meteo",
                    "interdiction_pulv", "traitement_deconseille", "pluie_limitante",
                    "risque_dispersion", "wind_speed_10m_mean", "precipitation_sum"
                ]),
                left_on="insee_com", right_on="code_insee", how="left"
            )
            .with_columns([
                (pl.col("ift_journalier_total") * pl.col("indicateur_meteo")).alias("risque_brut"),
                pl.lit(d).alias("date"),
            ])
        )
        resultats.append(risque_jour)

    df_prev = pl.concat(resultats)

    # Normalisation 
    q1, q2, q3 = VALEURS_SEUIL
    df_prev = df_prev.with_columns(
        pl.when(pl.col("risque_brut").is_null()).then(pl.lit(None, dtype=pl.Int32))
        .when(pl.col("risque_brut") == 0).then(pl.lit(0, dtype=pl.Int32))
        .when(pl.col("risque_brut") <= q1).then(pl.lit(1, dtype=pl.Int32))
        .when(pl.col("risque_brut") <= q2).then(pl.lit(2, dtype=pl.Int32))
        .when(pl.col("risque_brut") <= q3).then(pl.lit(3, dtype=pl.Int32))
        .otherwise(pl.lit(4, dtype=pl.Int32))
        .alias("risque_0_4")
    )

    return df_prev.sort(["insee_com", "date"])


# ============================================================
# 7. SAUVEGARDE DUCKDB
# ============================================================
def write_risque_to_duckdb(df: pl.DataFrame, annee: int, region: str | None = None):
    """
    Écrit le risque journalier dans la table DuckDB `risque_journalier`.
    Stratégie : supprime les lignes existantes pour l'année + communes concernées,
    puis insère les nouvelles (permet les mises à jour partielles par région).
    """
    con = get_duckdb()
    con.execute("""
        CREATE TABLE IF NOT EXISTS risque_journalier (
            insee_com            VARCHAR,
            date                 DATE,
            ift_journalier_total DOUBLE,
            indicateur_meteo     INTEGER,
            interdiction_pulv    BOOLEAN,
            traitement_deconseille BOOLEAN,
            pluie_limitante      BOOLEAN,
            risque_dispersion    BOOLEAN,
            wind_speed_10m_mean   DOUBLE,
            precipitation_sum    DOUBLE,
            risque_brut          DOUBLE,
            risque_0_4           INTEGER,
            PRIMARY KEY (insee_com, date)
        )
    """)
    communes_str = "', '".join(df["insee_com"].unique().to_list())
    con.execute(f"""
        DELETE FROM risque_journalier
        WHERE year(date) = {annee}
          AND insee_com IN ('{communes_str}')
    """)
    con.execute("""
        INSERT INTO risque_journalier
        SELECT insee_com, date, ift_journalier_total, indicateur_meteo,
               interdiction_pulv, traitement_deconseille, pluie_limitante, risque_dispersion,
               wind_speed_10m_mean, precipitation_sum, risque_brut, risque_0_4
        FROM df
    """)
    n = df.shape[0]
    region_label = f" — {region}" if region else ""
    logger.info(
        f"✓ DuckDB risque_journalier{region_label} — {n:,} lignes "
        f"({df['date'].n_unique()} jours × {df['insee_com'].n_unique()} communes)"
    )
    con.close()


def write_previsions_to_duckdb(df: pl.DataFrame):
    """
    Écrit les prévisions dans la table DuckDB `risque_previsions`.
    La table est entièrement remplacée à chaque run (les prévisions ne sont pas historisées).
    """
    con = get_duckdb()
    con.execute("DROP TABLE IF EXISTS risque_previsions")
    con.execute("""
        CREATE TABLE risque_previsions (
            insee_com            VARCHAR,
            date                 DATE,
            ift_journalier_total DOUBLE,
            indicateur_meteo     INTEGER,
            interdiction_pulv    BOOLEAN,
            traitement_deconseille BOOLEAN,
            pluie_limitante      BOOLEAN,
            risque_dispersion    BOOLEAN,
            wind_speed_10m_mean   DOUBLE,
            precipitation_sum    DOUBLE,
            risque_brut          DOUBLE,
            risque_0_4           INTEGER,
            PRIMARY KEY (insee_com, date)
        )
    """)
    con.execute("""
        INSERT INTO risque_previsions
        SELECT insee_com, date, ift_journalier_total, indicateur_meteo,
               interdiction_pulv, traitement_deconseille, pluie_limitante, risque_dispersion,
               wind_speed_10m_mean, precipitation_sum, risque_brut, risque_0_4
        FROM df
    """)
    logger.info(
        f"✓ DuckDB risque_previsions — {df.shape[0]:,} lignes "
        f"({df['date'].n_unique()} jours × {df['insee_com'].n_unique()} communes)"
    )
    con.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--annee", type=int, default=2025)
    parser.add_argument("--region", type=str, default=None,
                        help='Filtrer sur une région (ex: "Pays de la Loire")')
    parser.add_argument("--previsions", action="store_true",
                        help="Calcule aussi le risque prévisionnel (7 jours)")
    parser.add_argument("--methode", type=str, default="quartiles", choices=["quartiles", "valeurs"],
                        help="Méthode de discrétisation du risque (quartiles ou seuils fixes)")
    args = parser.parse_args()

    region_label = f" — {args.region}" if args.region else ""
    logger.info(f"Calcul risque journalier {args.annee}{region_label}...")
    df = compute_risque_journalier(args.annee, args.region)

    write_risque_to_duckdb(df, args.annee, args.region)

    if args.previsions:
        logger.info(f"Calcul risque prévisionnel{region_label}...")
        df_prev = compute_risque_previsions(args.annee, args.region)
        if df_prev is not None:
            write_previsions_to_duckdb(df_prev)