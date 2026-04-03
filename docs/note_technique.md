# Note technique — Indicateur de risque de dispersion des pesticides
**Projet PestiExpo — FNE**
*Version 1.1 — Mars 2026*

---

## 1. Architecture générale

### 1.1 Stack technique

| Composant | Technologie | Rôle |
|---|---|---|
| ETL | Python 3.11, Polars, GeoPandas | Traitement des données brutes |
| Base de données | DuckDB | Stockage intermédiaire, vues SQL |
| Format d'échange | Parquet (Apache) | Stockage persistant des tables |
| Dashboard | Streamlit + Plotly | Visualisation interactive |
| API REST | FastAPI + Uvicorn | Exposition des données |
| Gestion de paquets | uv | Environnement Python reproductible |

### 1.2 Flux de données

```
data/raw/                         data/parquet/
├── ADONIS IFT CSV    ──────┐     ├── communes_admin.parquet
├── Calendrier XLSX   ─────►│ETL  ├── ift_communes_enrichi.parquet
├── IGN Admin Express ──────┘     ├── calendrier_epandage.parquet
└── RPG GPKG                      └── risque_journalier_{annee}.parquet
                                              │
                        ┌─────────────────────┤
                        ▼                     ▼
                 dashboard/app.py         api/main.py
                 (Streamlit)              (FastAPI)
```

### 1.3 Ordre d'exécution

```bash
# 1. ETL statique (annuel) — ~5 min
uv run python3 etl/etl_statique.py --annee 2026

# 2. Calcul de risque (annuel) — ~10-20 min selon le nb de communes
uv run python3 etl/calcul_risque_journalier.py --annee 2025

# 3. Initialisation des vues DuckDB (après chaque ETL statique)
uv run python3 etl/init_duckdb.py

# 4. Lancement du dashboard
uv run streamlit run dashboard/app.py

# 5. Lancement de l'API (optionnel)
cd api && uv run uvicorn main:app --reload
```

---

## 2. Sources de données brutes

| Fichier | Format | Taille | Description |
|---|---|---|---|
| `fre-324510908-adonis-ift-2022-v04112024.csv` | CSV (sep `;`) | 5,6 Mo | IFT ADONIS par commune (INRAE 2022) |
| `calendrier_culture_harmonise.xlsx` | XLSX | 49 Ko | Calendrier épandage — onglet "CVDL + PDL" |
| `ADE_4-0_GPKG_WGS84G_FRA-ED2026-02-16.gpkg` | GeoPackage | 1,1 Go | Communes IGN Admin Express (WGS84) |
| `RPG_3-0__GPKG_LAMB93_FXX_2024-01-01/` | GeoPackage | — | Registre Parcellaire Graphique France entière |
| `RPG_nomenclatures.xlsx` | XLSX | 18 Ko | Nomenclature codes cultures RPG (Annexes A, B, C) |
| `pesticides_2002_2023_v07_2025.xlsx` | XLSX | 66 Mo | Mesures atmosphériques pesticides |

---

## 3. ETL statique (`etl/etl_statique.py`)

### 3.1 Communes (`build_communes`)

- Source : couche `COMMUNE` du GeoPackage IGN WGS84
- Calcul des centroïdes : reprojection Lambert 93 → WGS84 via GeoPandas
- Colonnes produites : `code_insee`, `nom_commune`, `code_insee_dep`, `code_insee_reg`, `longitude`, `latitude`
- Sortie : `communes_admin.parquet` (~35 000 lignes)

### 3.2 IFT communes (`build_ift`)

- Source : `fre-324510908-adonis-ift-2022-v04112024.csv`
- Colonnes clés extraites :
  ```
  insee_com, sau, sau_bio, p_bio, p_bc, p_sau,
  c_maj, c_ift_hbc, c_ift_h,        ← noms de cultures (ADONIS)
  cod_c_maj, cod_c_hbc, cod_c_h,    ← codes RPG
  ift_t, ift_t_hbc, ift_h,          ← IFT annuels
  ift_t_hh, ift_hh_hbc, iftbc       ← IFT hors homologué, bio, conventionnel
  ```
- Enrichissement : libellés cultures via `RPG_nomenclatures.xlsx` (Annexe A)
- Code département dérivé des 2 premiers caractères de `insee_com`
- Jointure avec communes → `ift_communes_enrichi.parquet`

### 3.3 Calendrier d'épandage (`build_calendrier`)

- Source : `calendrier_culture_harmonise.xlsx`, onglet "CVDL + PDL"
- Colonnes : `departement_code`, `culture`, `Début de période`, `Fin de période`, `Herbicides`, `Fongicides`, `Insecticides`
- Normalisation des booléens ("Oui"/"Non" → True/False)
- **Les dates sont stockées avec l'année passée en argument** (ex. `--annee 2026`). Lors du calcul de risque pour une autre année, les mois/jours sont conservés mais l'année est recalée via `make_date({annee}, month(), day())`.
- Sortie : `calendrier_epandage.parquet` (~200 lignes)

---

## 4. Calcul de risque journalier (`etl/calcul_risque_journalier.py`)

### 4.1 Table de correspondance des cultures

La fonction `normaliser_culture()` applique le dictionnaire `CULTURE_MAPPING` (~60 entrées) via `pl.Expr.replace_strict()` pour aligner les noms ADONIS avec les noms du calendrier. Les cultures sans correspondance retournent `None` (IFT journalier = 0).

> `CULTURE_MAPPING` est défini dans `etl/config.py` (importé via `from config import *`) afin d'être partagé entre `calcul_risque_journalier.py` et `dashboard/app.py`.

Exemples de mapping :

| Nom ADONIS | Nom calendrier |
|---|---|
| Triticale, Épeautre, Seigle d'hiver | Céréales d'hiver |
| Lin fibres, Lin oléagineux | Lin printemps |
| Bois pâturé, Prairie permanente | Prairies |
| Agrume, Oliveraie, Vergers | Arboriculture |
| Vigne, Vigne : raisins de cuve | Vigne |
| Soja | Féverole d'hiver |

### 4.2 Chargement des données (`load_data`)

```python
def load_data(annee: int) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame | None]:
```

**IFT** : chargé depuis la vue DuckDB `ift_communes_enrichi`, colonnes clés :
- `insee_com`, `code_insee_dep`
- `c_maj` / `ift_t_hbc` — IFT hors herbicides pour la culture principale
- `c_ift_hbc` / `ift_hh_hbc` — IFT hors homologué hors herbicides pour la culture fongi/insecti
- `c_ift_h` / `ift_h` — IFT herbicides
- Après chargement, trois colonnes normalisées ajoutées : `c_maj_cal`, `c_ift_hbc_cal`, `c_ift_h_cal`

**Calendrier** : chargé depuis `calendrier_epandage`, avec recalage de l'année :
```sql
SELECT
    departement_code, culture,
    make_date({annee}, month(Debut_de_periode), day(Debut_de_periode)) AS Debut_de_periode,
    make_date({annee}, month(Fin_de_periode),   day(Fin_de_periode))   AS Fin_de_periode,
    Herbicides, Fongicides, Insecticides,
    COUNT(*) FILTER (WHERE Herbicides = true)
        OVER (PARTITION BY departement_code, culture) AS nb_periodes_herbicides,
    COUNT(*) FILTER (WHERE Fongicides = true OR Insecticides = true)
        OVER (PARTITION BY departement_code, culture) AS nb_periodes_fongi_insecti,
    COUNT(*) OVER (PARTITION BY departement_code, culture) AS nb_periodes_total
FROM calendrier_epandage
```

> **Point critique** : le calendrier parquet peut être stocké avec une année différente de l'année de calcul (ex. généré avec `--annee 2026` mais calcul pour 2025). Le `make_date()` corrige ce décalage en ne conservant que le mois et le jour.

### 4.3 IFT journalier (`compute_ift_journalier`)

Pour chaque jour de l'année, la fonction effectue 3 passes (une par culture caractéristique) :

```python
configs = [
    ("c_maj_cal",     "ift_maj_hbc", None,                                          "nb_periodes_total"),
    ("c_ift_hbc_cal", "ift_hh_hbc",  pl.col("Fongicides") | pl.col("Insecticides"), "nb_periodes_fongi_insecti"),
    ("c_ift_h_cal",   "ift_h",       pl.col("Herbicides"),                          "nb_periodes_herbicides"),
]
```

Logique appliquée pour chaque passe :
1. `cal_active` = périodes contenant la date, filtrées par type de traitement (aucun filtre pour `c_maj`, `Fongicides|Insecticides` pour `c_ift_hbc`, `Herbicides` pour `c_ift_h`)
2. Join IFT ↔ `nb_periodes` (dénominateur spécifique par culture : `nb_periodes_total`, `nb_periodes_fongi_insecti`, `nb_periodes_herbicides`)
3. Join IFT ↔ `cal_active` pour détecter si une période est active
4. Trois cas distincts :
   - `nb_periodes IS NULL` → **`None`** (pas d'entrée dans le calendrier pour cette culture × département)
   - `periode_active IS NOT NULL` → `IFT_annuel / nb_periodes` (période active)
   - sinon → **`0.0`** (culture dans le calendrier, mais aucun traitement ce jour)

> **Choix de modélisation** : chaque culture est pondérée par le nombre de périodes du type correspondant à sa colonne IFT. `c_ift_hbc` est filtré sur les périodes fongicides ou insecticides car son IFT (`ift_hh_hbc`) ne couvre pas les herbicides. `c_ift_h` est filtré exclusivement sur les périodes herbicides. `c_maj` utilise l'ensemble des périodes sans distinction.

**Agrégation avec déduplication** : les trois contributions sont calculées séparément (`ift_j_maj`, `ift_j_hbc`, `ift_j_h`), puis jointes côte à côte. Deux flags de déduplication sont calculés depuis les colonnes `_cal` :

```python
hbc_different = (c_ift_hbc_cal != c_maj_cal).fill_null(True)
h_different   = (c_ift_h_cal   != c_maj_cal).fill_null(True)

hh_val = ift_j_hh if hbc_different else None
h_val  = ift_j_h  if h_different   else None

# None si aucune culture contributrice n'a de données calendrier
# sinon somme des contributions (null → 0 pour les cultures présentes)
if any(v is not None for v in [ift_j_maj, hh_val, h_val]):
    ift_journalier_total = (ift_j_maj or 0) + (hh_val or 0) + (h_val or 0)
else:
    ift_journalier_total = None
```

`fill_null(True)` : si une culture n'a pas de correspondance calendrier, elle est traitée comme différente (contribution = 0 de toute façon, aucune jointure calendrier possible).

> **Choix de modélisation** : la comparaison se fait sur les noms calendrier (après `CULTURE_MAPPING`), pas sur les noms ADONIS bruts. Deux cultures ADONIS qui mappent vers la même entrée calendrier utilisent les mêmes périodes et le même IFT de référence — les traiter comme identiques évite le double-comptage.

### 4.4 Indicateur météo (`compute_indicateur_meteo`)

Activé uniquement si `METEO_ENABLED=True` (voir `config.py`). Retourne un entier 0–3 (`Int32`) par commune.

| Condition (ordre de priorité) | `indicateur_meteo` | Flags associés |
|---|---|---|
| `precipitation_sum > PLUIE_SEUIL (0 mm)` **ou** `vent ≥ VENT_MAX (19 m/s)` | 0 | `pluie_limitante` ou `interdiction_pulv = True` |
| `vent ≥ VENT_DISPERSION_SEUIL2 (11 m/s)`, pas de pluie | 3 | `risque_dispersion = True` |
| `vent ≥ VENT_DISPERSION_MIN (5 m/s)`, pas de pluie | 2 | `risque_dispersion = True` |
| Sinon (vent < 5 m/s, pas de pluie) | 1 | — |

Sans météo : `indicateur_meteo = 1` pour toutes les communes, les flags sont `False`, `wind_speed_10m_max` et `precipitation_sum` sont `NULL`.

> **Note** : la condition pluie utilise `> PLUIE_SEUIL` (strictement supérieur) et non `>=`. Avec `PLUIE_SEUIL=0`, seules les précipitations réellement observées (> 0 mm) déclenchent l'annulation de la dispersion.

### 4.5 Normalisation 0–4 (`normalize_0_4`)

Calculée sur l'ensemble des valeurs `risque_brut > 0` de toute l'année × toutes communes :
- Q1, Q2, Q3 calculés avec `pl.Series.quantile()`
- `risque_brut == 0` → `risque_0_4 = 0`
- `0 < risque_brut ≤ Q1` → `1`
- `Q1 < risque_brut ≤ Q2` → `2`
- `Q2 < risque_brut ≤ Q3` → `3`
- `risque_brut > Q3` → `4`

### 4.6 Sortie

Fichier : `data/parquet/risque_journalier_{annee}.parquet`

| Colonne | Type | Description |
|---|---|---|
| `insee_com` | Utf8 | Code INSEE commune |
| `date` | Date | Date de calcul |
| `ift_journalier_total` | Float64 | Somme des 3 IFT journaliers |
| `risque_brut` | Float64 | `ift_journalier_total × indicateur_meteo` |
| `risque_0_4` | Int32 | Indicateur normalisé 0–4 |
| `indicateur_meteo` | Int32 | Indicateur météo 0–3 (1 si METEO_ENABLED=False) |
| `interdiction_pulv` | Boolean | Vent ≥ VENT_MAX ou pluie > PLUIE_SEUIL |
| `pluie_limitante` | Boolean | Pluie > PLUIE_SEUIL |
| `risque_dispersion` | Boolean | Vent ≥ VENT_DISPERSION_MIN |
| `wind_speed_10m_max` | Float64 | NULL si METEO_ENABLED=False |
| `precipitation_sum` | Float64 | NULL si METEO_ENABLED=False |

---

## 5. Base de données DuckDB (`etl/init_duckdb.py`)

DuckDB est utilisé comme moteur SQL local sur les fichiers Parquet. Il ne stocke pas de données propres : ses **vues** pointent directement vers les fichiers Parquet.

> **Chemins absolus obligatoires** : `DATA_DIR` est résolu en chemin absolu via `Path.resolve()` dans `config.py`. Sans cela, les vues fonctionnent en ligne de commande mais échouent depuis DBeaver ou d'autres outils avec un CWD différent.

### Vues disponibles

| Vue | Source Parquet |
|---|---|
| `communes` | `communes_admin.parquet` |
| `ift_communes` | `ift_communes.parquet` |
| `ift_communes_enrichi` | `ift_communes_enrichi.parquet` |
| `calendrier_epandage` | `calendrier_epandage.parquet` |
| `nomenclature_annexe_a/b/c` | `nomenclature_annexe_*.parquet` |
| `indicateurs_meteo` | Parquet Hive partitionné `meteo/historique/annee=*/` |
| `indicateurs_previsions` | Parquet Hive partitionné `meteo/previsions/` |

### Connexion API

L'API FastAPI utilise DuckDB **en mémoire** (`:memory:`) afin d'éviter les conflits de verrou de fichier avec les processus ETL :

```python
con = duckdb.connect(":memory:")
con.execute(f"CREATE VIEW IF NOT EXISTS communes AS SELECT * FROM read_parquet('{path}')")
```

---

## 6. Dashboard Streamlit (`dashboard/app.py`)

### 6.1 Chargement des données

- `load_communes()` : join `communes_admin` + `ift_communes_enrichi` + calendrier, calcul du flag `has_calendar_data`
- `load_risque(annee)` : lecture du parquet `risque_journalier_{annee}.parquet`
- Les deux fonctions sont décorées `@st.cache_data` pour éviter les rechargements

### 6.2 Logique d'état (`get_state`)

```python
def get_state(risque_0_4, ift_total, has_cal: bool) -> str | int:
    if not has_cal:    return "no_calendar"
    if pd.isna(risque_0_4): return "no_data"
    return int(risque_0_4)
```

### 6.3 Cartographie

- Plotly `Choroplethmap` (MapLibre) avec fond "carto-positron"
- Géométries des communes issues de `communes_geo.geojson` (contours simplifiés à 0.001°, généré par `build_communes_geo()` dans `etl_statique.py`)
- Une seule trace choroplèthe avec une colorscale discrète à 7 niveaux (états : no_calendar, no_data, 0 à 4)
- Légende reconstituée via des traces `Scattermap` invisibles (`lat=[None]`)
- Commune sélectionnée : seconde trace `Choroplethmap` avec bordure blanche épaisse
- Fallback sur `Scattermap` (centroïdes) si le GeoJSON est absent
- Zoom adaptatif : 5 (national) → 6 (région) → 8 (département)

### 6.4 Rafraîchissement après recalcul

Le dashboard relit les parquets à chaque requête. Après un recalcul ETL, vider le cache via le menu ☰ → *Clear cache* ou redémarrer Streamlit.

---

## 7. API REST (`api/`)

### 7.1 Structure

```
api/
├── main.py          # App FastAPI, CORS, montage des routers
├── db.py            # Connexion DuckDB :memory: + cache communes_ref()
├── schemas.py       # Modèles Pydantic v2
└── routers/
    ├── communes.py  # GET /communes, /communes/{code}
    ├── risque.py    # GET /risque/carte/{date}, /risque/{code}[/{date}]
    ├── calendrier.py # GET /calendrier, /calendrier/cultures
    └── mesures.py   # GET /mesures/{code}
```

### 7.2 Endpoints principaux

| Méthode | Endpoint | Description |
|---|---|---|
| GET | `/health` | Statut, nb communes, années disponibles |
| GET | `/communes` | Liste avec filtres région/dept, pagination |
| GET | `/communes/{code_insee}` | Détail commune + IFT + calendrier |
| GET | `/risque/carte/{date}` | Risque toutes communes à une date |
| GET | `/risque/{code_insee}` | Série temporelle commune |
| GET | `/risque/{code_insee}/{date}` | Risque pour une commune à une date |
| GET | `/calendrier` | Calendrier d'épandage filtrable |
| GET | `/mesures/{code_insee}` | Mesures atmosphériques par station |

### 7.3 Lancement

```bash
cd api
uv run uvicorn main:app --reload --port 8000
# Documentation interactive : http://localhost:8000/docs
```

---

## 8. Configuration (`etl/config.py`)

| Variable | Défaut | Description |
|---|---|---|
| `DATA_DIR` | `./data` (résolu absolu) | Répertoire racine des données |
| `METEO_ENABLED` | `False` | Active la logique météo |
| `VENT_MAX` | `19` m/s | Seuil d'interdiction de pulvérisation (indicateur = 0) |
| `VENT_DISPERSION_SEUIL2` | `11` m/s | Seuil forte dispersion (indicateur = 3) |
| `VENT_DISPERSION_MIN` | `5` m/s | Seuil dispersion modérée (indicateur = 2) |
| `PLUIE_SEUIL` | `0` mm | Seuil pluie limitante (`precipitation_sum > 0` → indicateur = 0) |
| `METHODE_SEUIL` | `"quartiles"` | Méthode de discrétisation 0–4 (`"quartiles"` ou `"valeurs"`) |
| `VALEURS_SEUIL` | `(1, 2, 3)` | Seuils fixes si `METHODE_SEUIL="valeurs"` |
| `METEO_CHUNK_SIZE` | `100` | Communes par requête API Open-Meteo |

Toutes ces variables sont surchargeable par variables d'environnement.

---

## 9. Problèmes connus et résolutions

### 9.1 Décalage d'année dans le calendrier

**Symptôme** : indicateur = 0 pour toutes les communes toute l'année.

**Cause** : `etl_statique.py` exécuté avec `--annee 2026` stocke le calendrier avec des dates 2026. Le calcul pour 2025 filtrait autrefois `WHERE YEAR(Debut_de_periode) = 2025` → 0 lignes.

**Résolution** : remplacement du filtre par `make_date({annee}, month(), day())` dans `load_data()`. Le calendrier est normalisé vers l'année de calcul quel que soit le millésime du parquet.

### 9.2 Mauvaise colonne de culture dans `compute_ift_journalier`

**Symptôme** : la jointure sur le calendrier ne trouve aucune correspondance car les noms ADONIS bruts sont utilisés.

**Cause** : utilisation de `c_maj`, `c_ift_hbc`, `c_ift_h` (noms ADONIS) au lieu de `c_maj_cal`, `c_ift_hbc_cal`, `c_ift_h_cal` (noms calendrier).

**Résolution** : refactoring de `compute_ift_journalier` avec le tableau `configs` utilisant les colonnes `_cal` et les colonnes `nb_periodes` spécifiques par type de traitement.

### 9.3 Vues DuckDB vides dans DBeaver

**Symptôme** : les vues apparaissent dans DBeaver mais retournent 0 lignes.

**Cause** : DBeaver résout les chemins relatifs depuis son propre répertoire de travail, différent du répertoire du projet.

**Résolution** : `DATA_DIR = Path(...).resolve()` dans `config.py` — les chemins dans les vues DuckDB sont maintenant absolus.

### 9.4 Encodage des caractères accentués (mesures)

**Symptôme** : les accents dans les noms de substances apparaissent en caractères illisibles.

**Résolution** : ajout de `engine="openpyxl"` dans `pl.read_excel()` pour le fichier des mesures pesticides.

---

## 10. Dépendances principales

```toml
[project.dependencies]
duckdb       = ">=0.10"
polars       = ">=0.20"
geopandas    = ">=0.14"
fastapi      = ">=0.110"
streamlit    = ">=1.32"
plotly       = ">=5.20"
pyarrow      = ">=15.0"
uvicorn      = ">=0.28"
openpyxl     = ">=3.1"
pyproj       = ">=3.6"    # conversion Lambert 93 → WGS84 (mesures)
```

---

*Document produit dans le cadre du projet PestiExpo — ARCOOP / FNE — Mars 2026*
