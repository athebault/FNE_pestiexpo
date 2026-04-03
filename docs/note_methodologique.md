# Note méthodologique — Indicateur de risque d'exposition aux pesticides atmosphériques

**Projet PestiExpo — FNE**  
*Version 1.1 — Avril 2026*

* * *

## 1\. Contexte et objectifs

Le projet PestiExpo vise à produire un indicateur journalier d'exposition potentielle aux pesticides agricoles dans l’atmosphère, à l'échelle communale, sur l'ensemble du territoire français métropolitain. L'objectif est de permettre à tout citoyen de savoir s'il est exposé à un risque potentiel un jour donné, et à FNE (France Nature Environnement) d'alimenter son plaidoyer pour des données plus fiables via des mesures sur le terrain et la mise en place de normes réglementaires sur la concentration en pesticides atmosphériques.

L'indicateur ne mesure pas une contamination observée, mais un **risque potentiel de traitement et de dispersion** calculé à partir de données privées (base de données ADONIS de Solagro) et publiques (pratiques agricoles, météorologie). Il répond à la question : *quel est le risque qu'une commune soit exposée à des pesticides un jour donné, compte tenu des pratiques agricoles locales et des conditions météorologiques ?*

* * *

## 2\. Sources de données

### 2.1 IFT par commune — base ADONIS (SOLAGRO)

L'Indicateur de Fréquence de Traitement (IFT) est la donnée centrale du modèle. Il est issu de la base **ADONIS** produite par l'association Solagro (données 2022), qui fournit pour chaque commune française :

- Les **trois cultures principales** sur le plan phytosanitaire :
    - `c_maj` : culture la plus représentée sur la commune (en surface agricole)
    - `c_ift_hbc` : culture ayant l'IFT hors herbicides et hors produits de biocontrôle le plus élevé (fongicides + insecticides)
    - `c_ift_h` : culture ayant l'IFT herbicides le plus élevé
- Les **valeurs d'IFT annuelles** associées (moyennes à l'échelle de la commune) :
    - `ift_t_hbc` : IFT total hors produits de biocontrôle (herbicides + fongicides + insecticides)
    - `ift_hh_hbc` : IFT fongicides + insecticides, hors produits de biocontrôle
    - `ift_h` : IFT herbicides
- Des indicateurs de surface (SAU, part en agriculture biologique, etc.)

L'IFT est une unité sans dimension qui représente le nombre de doses de référence appliquées par hectare et par an. Par exemple, un IFT de 4 signifie que la culture a reçu l'équivalent de 4 traitements à la dose recommandée.

### 2.2 Calendrier d'épandage harmonisé

Un calendrier d'épandage a été constitué par FNE à partir de données terrain (Bulletin de Santé du Végétal (BSV) notamment) et d'expertise métier. Il liste, pour chaque **culture** et chaque **département**, les **périodes de traitement** au cours de l'année, en distinguant trois types de traitements :

- **Herbicides** : traitements contre les mauvaises herbes
- **Fongicides** : traitements contre les champignons
- **Insecticides** : traitements contre les insectes ravageurs

Ce calendrier est la clé de répartition temporelle de l'IFT annuel : il indique *quand* les traitements ont lieu au cours de l'année.

### 2.3 Référentiel géographique des communes (IGN)

Les contours et centroïdes des communes françaises sont issus du produit **Admin Express** de l'IGN (édition 2024). Les coordonnées sont calculées en projection Lambert 93 puis converties en WGS84 pour l'affichage cartographique.

### 2.4 Données météorologiques (optionnel)

Les données météorologiques horaires et journalières (vitesse du vent, précipitations) peuvent être intégrées via l'API **Open-Meteo** (archives historiques) et l'API **MeteoFrance** (prévisions). Cette dimension est actuellement désactivée par défaut (`METEO_ENABLED=False`) du fait de la difficulté de récupération des données météorologique à l'échelle communale sur toute la France.

Une autre méthode de téléchargement des données permet de récupérer rapidement les données historique en local à partir du Modele ERA5 de Copernicus
https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview

### 2.5 Mesures atmosphériques de pesticides

Des mesures de concentration de pesticides dans l'air sont disponibles pour certaines stations de surveillance atmosphérique (2002-2023). Ces données viennent de faire l'objet de publication d'un observatoire [PhytAtmo Dataviz](https://www.atmo-france.org/actualite/phytatmo-dataviz-un-nouvel-outil-de-suivi-des-pesticides-dans-lair) et d'une [visualisation interactive en ligne.](https://storymaps.arcgis.com/stories/b5e04945978c4b0f876398812330d330) Ces données sont visualisables dans un module dédié du dashboard mais ne participent pas au calcul de l'indicateur de risque. Elles peuvent néanmoins être utilisées pour valider la pertinence de l'indicateur crée.

* * *

## 3\. Construction de l'indicateur

### 3.1 Principe général

L'indicateur repose sur l'idée que **le risque d'exposition est maximal les jours où des traitements sont appliqués**. La question centrale est donc : *un jour donné, y a-t-il des traitements en cours dans les cultures présentes dans la commune ?*

Pour répondre à cette question, on croise :

1.  L'IFT annuel de la commune (combien de traitements au total sur l'année ?)
2.  Le calendrier d'épandage (quand ces traitements sont-ils répartis dans l'année ?)
3.  La météo journalière (quel jour la dispersion des particules dans l'air est-elle la plus probable?)

### 3.2 IFT journalier par commune

Pour chaque commune et chaque jour de l'année, l'IFT journalier est calculé séparément pour les trois cultures caractéristiques :

**Si la culture n'a pas d'entrée dans le calendrier** (culture hors référentiel ou département non couvert) :

```
IFT_journalier(culture, jour) = NaN  ← absence de données
```

**Si la date tombe dans une période de traitement active :**

```
IFT_journalier(culture, jour) = IFT_annuel(culture) / nb_periodes_de_traitement(culture, département)
```

**Si la culture est dans le calendrier mais hors période active ce jour :**

```
IFT_journalier(culture, jour) = 0  ← pas de traitement ce jour
```

La distinction entre `NaN` (absence de données calendrier) et `0` (pas de traitement ce jour) est maintenue jusqu'à l'indicateur final afin de permettre une représentation visuelle différenciée et pouvoir visualiser facilement sur la carte finale l'absence de données.

Le calcul est répété pour les trois cultures (`c_maj`, `c_ift_hbc`, `c_ift_h`), selon la logique suivante :

| Culture | IFT utilisé | Périodes actives | Dénominateur |
| --- | --- | --- | --- |
| `c_maj` — culture à l'IFT total le plus fort | `ift_t_hbc` (IFT total hors biocontrole) | Toutes les périodes | Nb total de périodes de traitement |
| `c_ift_hbc` — culture à l'IFT fongi/insecti le plus fort | `ift_hh_hbc` (IFT hors herbicides, hors biocontrôle) | Périodes fongicides ou insecticides uniquement | Nb de périodes de traitement fongicides et/ou insecticides |
| `c_ift_h` — culture à l'IFT herbicides le plus fort | `ift_h` (IFT herbicides) | Périodes herbicides uniquement | Nb de périodes de traitement herbicides |

La distinction par type de traitement est appliquée pour les trois cultures : `c_ift_h` utilise exclusivement les périodes herbicides, `c_ift_hbc` les périodes fongicides ou insecticides, et `c_maj` l'ensemble des périodes du calendrier sans filtre par type.

L'IFT journalier total de la commune est la **somme des contributions non redondantes** :

```
IFT_journalier_total = IFT_j(c_maj)
    + IFT_j(c_ift_hbc)  si c_ift_hbc ≠ c_maj (noms calendrier)
    + IFT_j(c_ift_h)    si c_ift_h ≠ c_maj (noms calendrier)
```

La comparaison est faite sur les **noms du calendrier** de traitement fourni par FNE (après normalisation ADONIS → calendrier) : deux cultures ADONIS distinctes qui mappent vers la même entrée calendrier (ex. "Triticale" et "Épeautre" → "Céréales d'hiver") sont considérées identiques et ne sont comptées qu'une fois. Cette règle évite de gonfler artificiellement le risque lorsqu'une même culture domine plusieurs classements IFT.

### 3.3 Correspondance entre nomenclatures

Les cultures ADONIS et les cultures du calendrier d'épandage utilisent des nomenclatures différentes. Une table de correspondance d'environ 60 entrées a été construite pour aligner les deux référentiels. Par exemple :

- "Triticale" (ADONIS) → "Céréales d'hiver" (calendrier)
- "Vigne : raisins de cuve" → "Vigne"
- "Bois pâturé" → "Prairies"
- "Agrume" / "Oliveraie" → "Arboriculture"

Les cultures ADONIS sans correspondance dans le calendrier sont signalées comme "hors calendrier" et reçoivent un IFT journalier nul.

### 3.4 Prise en compte de la météo

Lorsque les données météorologiques sont activées (`METEO_ENABLED=True`), un **indicateur météo discret** (entier de 0 à 3) est calculé à partir de la vitesse de vent et des précipitations journalières, puis multiplié à l'IFT journalier :

| Condition météo | Indicateur | Interprétation |
| --- | --- | --- |
| Pluie > 0 mm **ou** vent ≥ 19 m/s | 0   | Pas de dispersion : pluie lave les dépôts ou vent fort interdit la pulvérisation |
| Vent < 5 m/s, pas de pluie | 1   | Conditions calmes, faible risque de dérive |
| Vent 5–11 m/s, pas de pluie | 2   | Dispersion modérée |
| Vent 11–19 m/s, pas de pluie | 3   | Forte dispersion atmosphérique |

```
risque_brut = IFT_journalier_total × indicateur_meteo
```

Sans météo (mode par défaut, `METEO_ENABLED=False`), l'indicateur est fixé à **1** pour toutes les communes (conditions normales, sans amplification ni annulation).

### 3.5 Normalisation de l'indicateur (0 à 4)

L'IFT brut n'est pas directement interprétable par le grand public. Il est normalisé en **5 niveaux (0 à 4)** par la méthode des quartiles, calculés sur l'ensemble des valeurs positives de l'année pour toutes les communes :

| Valeur | Signification | Couleur |
| --- | --- | --- |
| 0   | Aucun traitement ce jour | Vert |
| 1   | Risque faible (< Q1) | Jaune |
| 2   | Risque modéré (Q1–Q2) | Orange |
| 3   | Risque élevé (Q2–Q3) | Orange foncé |
| 4   | Risque très élevé (> Q3) | Rouge |

Cette normalisation est **relative à l'année** : elle permet de comparer les communes entre elles et d'identifier les périodes de risque maximum au sein d'une année, sans prétendre à une valeur absolue d'exposition.

* * *

## 4\. Représentation et interprétation

### 4.1 États d'affichage

Le dashboard distingue cinq états possibles pour une commune à une date donnée :

| État | Signification |
| --- | --- |
| **Hors calendrier** | Les cultures de la commune ne sont pas couvertes par le calendrier d'épandage (gris) |
| **Données manquantes** | L'indicateur de risque n'a pas pu être calculé (gris clair) |
| **Aucun traitement (0)** | La commune est dans le calendrier mais aucun traitement n'est actif ce jour (vert) |
| **Risque 1 à 4** | Traitement actif, risque proportionnel à l'IFT (jaune à rouge) |

### 4.2 Lecture de l'indicateur

L'indicateur doit être lu comme un **signal de vigilance relatif**, non comme une mesure d'exposition absolue. Il est pertinent pour :

- Comparer le niveau de risque entre communes un même jour
- Identifier les périodes de l'année les plus à risque pour une commune donnée
- Détecter les territoires structurellement exposés (IFT élevé × cultures intensives)

* * *

## 5\. Limites et perspectives

### Limites actuelles

- **<ins>Limites méthodologiques</ins>**:
    - **Données IFT 2022** : les données ADONIS disponibles datent de 2022. La réalité agricole peut avoir évolué.
    - **Calendrier harmonisé** : le calendrier d'épandage est une approximation régionale ; les variations inter-annuelles et locales (météo de l'année, itinéraires techniques réels) ne sont pas capturées.
    - **Trois cultures par commune** : seules les cultures ayant les IFT les plus élevés sont prises en compte. Des cultures minoritaires à fort IFT peuvent être ignorées.
    - **Météo non activée par défaut** : l'indicateur météo est disponible mais désactivé (`METEO_ENABLED=False`). Sans cette dimension, le risque de dispersion effective n'est pas évalué ; seule la pression agronomique est mesurée.
    - **Normalisation relative** : la classification 0-4 est relative à l'année considérée. Un risque "4" en 2025 n'est pas nécessairement comparable à un risque "4" en 2020.
- <ins>**Limites techniques:**</ins>
    - **Données météorologiques**: actuellement, la récupération des données météorologique pour l'ensembles des communes présente une limite technique. Il est possible de télécharger les données historiques pour les années précédentes, mais la récupération des données pour l'année en cours peut poser problème du fait des contraintes en termes d'appels autorisés à l'API de Météo France. Il faut donc définir une stratégie pour la récupération de ces données. Une mise à jour journalière à l'ensemble du territoire peut éventuellement être programmée dans la nuit, par contre la mise à jour horaire devra probablement se faire à zone spatiale réduite (commune ou département).
    - **Calendrier d'épandage** : le calendrier d'épandage est complètement lié à la météorologie de l'année qui contraint le développement des cultures. Ce calendrier doit donc être mis à jour tous les ans, pour toutes les cultures dans tous les départements. Pendant l'année en cours, il doit être mis à jour régulièrement en fonction des nouvelles publications de BSV dans tous les départements. Les BSV n'étant pas harmonisés, cela risque d'entrainer un travail conséquent pour FNE. Le développement d'un module basé sur le développement des cultures (en degré jours) pourrait donc être pertinent à terme. Il serait intéressant de réaliser une étude bibliographiques sur les modèles actuellement disponibles pour les cultures annuelles et perennes et voir, en cas de modèles manquants, si une collaboration avec l'INRAE pourrait être mise en place pour le développement de ces modèles.

&nbsp;

* * *

*Document produit dans le cadre du projet PestiExpo — EcoLibres / FNE — Avril 2026*