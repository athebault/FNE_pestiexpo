# Note de préconisation technique — Déploiement et intégration des données
**Projet PestiExpo — FNE**
*Version 1.0 — Avril 2026*

---

## 1. Contexte

L'outil PestiExpo produit un indicateur journalier de risque de dispersion des pesticides par commune (35 000 communes × 365 jours). Ce document présente les options techniques pour rendre ces données accessibles à des outils tiers (tableaux de bord, SIG, outils de reporting) et pour automatiser leur mise à jour.

---

## 2. Architecture actuelle

```
ETL (Python)  ──►  Parquets  ──►  Dashboard Streamlit  (local)
                              ──►  API REST FastAPI      (local)
```

Dans l'état actuel, les deux interfaces (dashboard et API) tournent en local sur le poste de travail. Les données ne sont pas accessibles depuis l'extérieur.

---

## 3. Exposition des données via l'API REST

### 3.1 Ce que l'API permet

L'API REST (FastAPI) expose les indicateurs via des endpoints HTTP standard :

| Endpoint | Usage |
|---|---|
| `GET /risque/carte/{date}` | Risque de toutes les communes à une date donnée |
| `GET /risque/{code_insee}` | Série temporelle pour une commune |
| `GET /communes/{code_insee}` | Détail commune + cultures + IFT |
| `GET /calendrier` | Calendrier d'épandage filtrable |
| `GET /mesures/{code_insee}` | Mesures atmosphériques par station |

Ces endpoints retournent du **JSON standard**, compatible avec tous les outils de visualisation et de reporting du marché.

### 3.2 Intégration avec des outils tiers

| Outil | Mode de connexion | Fraîcheur possible |
|---|---|---|
| **Power BI** | Connecteur "Web" → URL de l'endpoint | À chaque actualisation du rapport |
| **Excel** | "Données depuis le Web" | Manuelle ou planifiée |
| **Tableau** | Connecteur Web Data Connector | À chaque actualisation |
| **Grafana** | Plugin JSON datasource | Quasi temps réel |
| **QGIS / SIG** | Requête HTTP + import GeoJSON/CSV | Manuelle |
| **Application web tierce** | Requêtes HTTP directes | Temps réel |

> **Condition préalable** : l'API doit être déployée sur un serveur accessible (voir section 4). En fonctionnement local, elle n'est accessible que depuis le poste de travail.

---

## 4. Options de déploiement

### 4.1 Option A — Serveur dédié (recommandé pour un usage pérenne)

Déploiement de l'API sur un serveur Linux (VPS, serveur FNE, cloud) via Docker :

```bash
# Construction de l'image
docker build -t pestiexpo-api ./api

# Lancement
docker run -d -p 8000:8000 \
  -v /chemin/vers/data:/app/data \
  pestiexpo-api
```

L'API est alors accessible à une URL fixe (ex. `https://pestiexpo.fne.asso.fr/api`).

**Avantages :** disponibilité permanente, accessible depuis Power BI / outils FNE sans intervention manuelle.

**Prérequis :** serveur Linux (2 Go RAM minimum), nom de domaine, certificat HTTPS.

### 4.2 Option B — Exposition temporaire depuis le poste local (pour tests)

Sans serveur, il est possible d'exposer temporairement l'API locale avec un tunnel :

```bash
# Avec ngrok (outil gratuit)
ngrok http 8000
# → génère une URL publique temporaire (ex. https://abc123.ngrok.io)
```

Adapté pour des démonstrations ou des tests d'intégration, pas pour un usage en production.

### 4.3 Option C — Export statique planifié (alternative légère)

Sans déploiement d'API, il est possible d'exporter les données en fichiers CSV/JSON chaque nuit et de les déposer sur un serveur de fichiers ou SharePoint accessible à Power BI.

```bash
# Exemple : export quotidien du risque pour la date du jour
uv run python3 etl/export_csv.py --date $(date +%Y-%m-%d)
```

**Avantages :** simple, pas de serveur applicatif, compatible avec les environnements sans infrastructure IT.

**Limites :** données moins fraîches (export différé), pas d'interrogation dynamique par commune ou période.

---

## 5. Automatisation de la mise à jour des données

Pour que les données exposées soient fraîches, la chaîne ETL doit tourner automatiquement. Les étapes à planifier (par exemple via `cron` sur Linux ou le Planificateur de tâches Windows) :

```
Chaque nuit (ex. 02h00) :
  1. etl_meteo_historique.py   ← récupère les données météo de la veille (Open-Meteo)
  2. calcul_risque_journalier.py --annee {annee_courante}  ← recalcule l'indicateur
  3. (optionnel) export_csv.py  ← si option C retenue
```

Exemple de crontab Linux :
```cron
0 2 * * * cd /opt/pestiexpo && uv run python3 etl/etl_meteo_historique.py
30 2 * * * cd /opt/pestiexpo && uv run python3 etl/calcul_risque_journalier.py --annee $(date +%Y)
```

> **Point d'attention** : le calcul de risque sur une année complète (~35 000 communes × 365 jours) prend 10 à 20 minutes. En production, il peut être optimisé pour ne recalculer que les derniers jours plutôt que l'année entière.

---

## 6. Recommandations selon le contexte FNE

| Scénario | Option recommandée |
|---|---|
| Usage interne FNE, quelques utilisateurs, accès au réseau FNE | **Option A** sur serveur interne FNE |
| Publication grand public ou partenaires externes | **Option A** sur VPS cloud avec HTTPS |
| Pas d'infrastructure disponible à court terme | **Option C** (exports CSV vers SharePoint/Drive) |
| Démonstration ponctuelle ou pilote | **Option B** (tunnel ngrok) |

---

## 7. Évolutions envisageables

- **Authentification** : ajout d'une clé API (`Bearer token`) pour restreindre l'accès
- **Cache** : mise en cache des réponses fréquentes (carte du jour) pour réduire la charge
- **Prévisions** : intégration des données météo prévisionnelles (déjà partiellement implémentées dans l'ETL) pour exposer un indicateur de risque à J+3 / J+7
- **Webhook / notification** : alerte automatique lorsque le risque dépasse un seuil dans un département donné

---

*Document produit dans le cadre du projet PestiExpo — ARCOOP / FNE — Avril 2026*
