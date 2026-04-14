# Diagnostic de faisabilité : mise en œuvre par FNE

**Projet** : PestiExpo — FNE
**Date** : avril 2026
**Version** : 1.0
**Auteure** : Aurélie Thébault - EcoLibres

---

## 1. Objet

Ce document évalue la faisabilité d'une mise en production autonome de l'outil PestiExpo par FNE, à partir du travail de conception réalisé dans le cadre de ce projet (cf. Livrable 3 — Conception de l'indicateur). Il ne décrit pas un prototype existant à reprendre, mais présente les briques techniques à assembler, les compétences et ressources nécessaires, et des recommandations pragmatiques selon différents scénarios.

Le périmètre envisagé pour une première mise en œuvre est **2 zones pilotes** (départements ou territoires à définir par FNE), avant toute extension nationale.

---

## 2. Ce que l'outil doit faire

L'outil PestiExpo doit calculer et diffuser un **indicateur journalier de risque d'exposition aux pesticides atmosphériques** (0 à 4) par commune, à partir de deux composantes décrites dans le Livrable 3 :

1. **Risque agronomique** : pression de traitement phytosanitaire estimée à partir des IFT ADONIS (Solagro) et du calendrier d'épandage FNE, pour 1 à 3 cultures représentatives par commune.
2. **Indicateur météo** : conditions de dispersion atmosphérique (vent + pluie) issues d'Open-Meteo / ERA5, à la maille ~9 km.

L'indicateur final est discrétisé en 5 niveaux (0 à 4) selon des **valeurs seuils absolues** (cf. §4.2.2 du Livrable 3), ce qui garantit une signification stable dans le temps et une communication claire.

FNE souhaite que la carte accesssible en ligne.
EcoLibre préconise que l'outil soit :
- accessible depuis un **navigateur web, y compris sur mobile** (usage terrain, extérieur, réseau 4G/5G)
- **mise à jour automatiquement chaque nuit** (données météo récentes + prévisions J+7)
- permettant de visualiser **l'historique** (depuis le début de l'année en cours) et les **prévisions** (7 jours)
- *Question pour FNE*: existe-t-il un réel besoin d'avoir les données historiques?

Ce que l'outil **ne fera pas** :
- Il ne prédit pas des concentrations de pesticides (pas un modèle physico-chimique).
- Il ne localise pas les sources d'exposition (habitations, écoles, cours d'eau exposés).
- Il ne couvre que les communes sur lesquelles des données sont disponibles.
- Il ne produit pas un indicateur validé scientifiquement par confrontation terrain.

---

## 3. Limites et biais de l'indicateur

Ces limites sont documentées en détail dans le Livrable 3 (§6 et §9). En voici les points les plus importants pour la communication et la prise de décision.

### 3.1 Limites principales

| Limite | Description | Gravité |
|--------|-------------|---------|
| IFT communal 2022 | Données ADONIS basées sur 2022. Les pratiques peuvent avoir évolué. Mise à jour possible tous les 2–3 ans. | Élevée |
| 3 cultures par commune | Seules les cultures majoritaire, à IFT hors biocontrôle et à IFT herbicide le plus élevé sont retenues. Cultures minoritaires mais très traitées potentiellement ignorées. | Moyenne |
| Uniformité des traitements | L'IFT annuel est réparti uniformément sur toute la période active du calendrier. Les pics de traitement réels ne sont pas capturés. | Élevée |
| Vent moyen journalier | Le vent au moment de l'épandage peut différer du vent moyen du jour. | Moyenne |
| Résolution météo (~9 km) | Les effets de relief local (vallée abritée, foehn) ne sont pas capturés. | Faible à moyenne |
| Couverture AASQA limitée | Seulement 182 communes disposent de mesures atmosphériques permettant une validation partielle. | Faible (comparaison uniquement) |

### 3.2 Biais structurels

- **Biais de sous-estimation** pour les petites cultures intensives (viticulture, maraîchage) sur faibles surfaces mais à fort IFT.
- **Biais de lissage temporel** : l'hypothèse d'IFT uniforme sur la période allonge artificiellement les durées de risque.
- **Absence de direction du vent** : l'indicateur ne dit pas *qui* est exposé selon la position par rapport aux parcelles traitées.

### 3.3 Signification de l'indicateur : seuils absolus (obligatoire avant publication)

Le Livrable 3 recommande d'utiliser des **valeurs seuils absolues** plutôt que les quartiles de la distribution nationale. Ce choix est fondamental pour la communication publique :

| Critère | Méthode quartiles | Méthode seuils absolus (recommandée) |
|---------|------------------|--------------------------------------|
| Signification d'un niveau 4 | "Parmi les 25 % de jours les plus à risque" — relatif | "Dose × dispersion dépassent les seuils définis" — absolu |
| Comparabilité inter-annuelle | Non | Oui |
| Calcul au fil de l'année | Difficile | Simple |
| Communication publique | Risque de sur-interprétation | Claire et honnête |

Les valeurs seuils proposées dans le Livrable 3 (indicateur agronomique : IFT ≤ 0,5 / ≤ 1 / ≤ 2 / ≤ 4 / > 4 ; indicateur météo : 0/1/2/3) devront être **validées par des experts** en agronomie et en qualité de l'air avant publication.

---

## 4. Architecture technique cible

Pour répondre aux besoins de FNE (carte mobile, mise à jour quotidienne, historique + prévisions), l'architecture cible se compose de trois couches indépendantes :

```
[Tâche nocturne automatique — chaque nuit vers 2h]
   ├── Récupération météo archives J-7 à J-1 (API Open-Meteo)
   ├── Récupération prévisions J à J+7 (API Open-Meteo)
   └── Calcul de l'indicateur de risque journalier → stockage base de données

[Serveur web]
   ├── API de données → répond aux requêtes du frontend (risque par commune/date)
   ├── Géométries des communes (tuiles vectorielles, voir §4.2)
   └── Frontend carte → interface utilisateur accessible sur mobile
```

### 4.1 Calcul de l'indicateur (ETL)

Le calcul de l'indicateur est le cœur du système. Il doit :

1. Croiser les IFT ADONIS avec le calendrier d'épandage pour produire un **IFT journalier** par commune (logique décrite dans le Livrable 3, §2).
2. Récupérer les variables météo journalières (vent moyen, précipitations) pour les centroïdes des communes via l'API Open-Meteo.
3. Calculer l'**indicateur météo** (0–3) et le **risque brut** (IFT journalier × indicateur météo).
4. Discrétiser en 5 niveaux (0–4) selon les seuils absolus définis.
5. Stocker les résultats dans une base de données (voir §4.3).

Pour les **2 zones pilotes**, ce calcul couvre quelques centaines à quelques milliers de communes au lieu de 34 000. Cela réduit significativement les ressources nécessaires (temps de calcul, stockage, bande passante de l'API météo).

### 4.2 Affichage cartographique : tuiles vectorielles

Pour une carte fluide sur mobile, il est indispensable d'utiliser des **tuiles vectorielles** plutôt qu'un fichier GeoJSON monolithique. La différence est simple : une carte classique charge la géométrie de toutes les communes en une fois (plusieurs dizaines de Mo) ; les tuiles vectorielles ne chargent que les communes visibles à l'écran au niveau de zoom courant, comme le fait Google Maps ou OpenStreetMap.

**Technologies recommandées :**
- **Génération des tuiles** : outil `tippecanoe` (gratuit, open source) — convertit le fichier de géométries des communes en fichier PMTiles.
- **Affichage** : **MapLibre GL JS** (bibliothèque cartographique open source, gratuite, standard du secteur). Interface web légère, fluide sur mobile, personnalisable.
- **Alternative plus simple** : **Leaflet.js** avec chargement progressif, moins performant mais plus facile à prendre en main pour un premier projet.

| Critère | GeoJSON classique | Tuiles vectorielles (PMTiles + MapLibre) |
|---------|-----------------|------------------------------------------|
| Poids chargé | Plusieurs dizaines de Mo (toutes les communes) | Quelques Ko (zone visible uniquement) |
| Fluidité sur mobile | Mauvaise | Très bonne |
| Zoom / navigation | Lent | Instantané |
| Complexité de mise en œuvre | Faible | Moyenne |

### 4.3 Base de données

À l'échelle nationale, le volume (~34 000 communes × 365 jours) représente environ **5–10 Go** de données. Une base de données légère de type **DuckDB** (fichier unique, pas de serveur à installer) suffit pour ce volume et pour la charge attendue d'un service public consultatif. Si le service montait en charge avec de nombreux utilisateurs simultanés, une migration vers **PostgreSQL** serait à envisager.

### 4.4 Mise à jour nocturne automatique

La mise à jour quotidienne des données météo et du calcul de l'indicateur doit être automatisée via une **tâche planifiée** (cron sur Linux, ou équivalent). La chaîne doit enchaîner :

1. Récupération des données météo archives (J-7 à J-1) — pour combler les données manquantes des jours précédents
2. Récupération des prévisions (J à J+7) — pour alimenter la vue "prévisions" de la carte
3. Calcul de l'indicateur de risque pour les nouvelles dates
4. **Alerte automatique** (email) si une étape échoue — indispensable pour détecter une panne sans surveillance manuelle

La carte doit afficher clairement la **date de dernière mise à jour** pour que l'utilisateur sache si les données sont fraîches.

---

## 5. Risques pour FNE

### 5.1 Risques techniques

| Risque | Probabilité | Impact | Mitigation |
|--------|-------------|--------|------------|
| Rupture de l'API Open-Meteo (changement de conditions d'accès) | Moyenne | Élevé | Prévoir un accès aux archives ERA5 (Copernicus) en solution de repli |
| IFT ADONIS non mis à jour par Solagro | Moyenne | Moyen | Prévoir de reconstruire depuis RPG + données AGRESTE si nécessaire |
| Calendrier d'épandage périmé | Faible à moyen | Moyen | Révision manuelle tous les 3–5 ans |
| Mise à jour nocturne qui échoue silencieusement | Élevée sans monitoring | Moyen | Mettre en place des alertes email dès le départ |
| Incompatibilité de librairies Python après mise à jour | Moyenne à long terme | Faible | Verrouiller les versions dans un environnement reproductible |

### 5.2 Risques d'usage et communication

- **Risque de sur-interprétation** : l'indicateur est un proxy du risque potentiel, pas une mesure de contamination. Un niveau 4 signifie "conditions de dose et de dispersion élevées", pas "danger sanitaire avéré". Une charte de communication stricte est indispensable avant toute publication.
- **Risque juridique** : désigner des communes comme "à risque élevé" sur la base d'un indicateur proxy peut avoir des conséquences. **Consulter un juriste avant toute diffusion publique.**
- **Risque de désaveu scientifique** : sans validation par un comité extérieur, l'indicateur peut être contesté par les acteurs agricoles ou les pouvoirs publics.

### 5.3 Risques opérationnels

- Si la personne référente technique quitte FNE, la maintenance devient très difficile sans documentation solide et sans compétences en interne.
- La mise à jour des données sources (IFT ADONIS, calendrier, RPG) est manuelle et nécessite un profil technique.
- Une mise à jour nocturne en panne non détectée peut laisser la carte avec des données périmées pendant plusieurs jours.

---

## 6. Compétences nécessaires

### Pour construire et maintenir l'outil

| Compétence | Niveau requis | Pourquoi |
|------------|---------------|----------|
| Python (Pandas ou Polars) | Intermédiaire | Calcul de l'indicateur, appels API météo, manipulation des données |
| SQL | Basique | Stocker et interroger les résultats dans la base de données |
| HTML / JavaScript (MapLibre GL ou Leaflet) | Basique à intermédiaire | Interface carte dans le navigateur |
| Linux / administration serveur | Basique | Déploiement, tâche cron, logs |
| Geopandas / géospatial | Basique | Traitement des géométries communales, génération des tuiles |

**Profil adapté** : développeur·se data ou web junior à intermédiaire, avec une sensibilité aux sujets agri-environnementaux. Ce profil est rare dans une association — la collaboration avec un prestataire ou une structure partenaire est à envisager, au moins pour la phase de construction initiale. Aurélie Thébault (EcoLibres) a les compétences et les partenaires pour répondre à ce besoin. 

### Pour les mises à jour de données

| Tâche | Profil minimum | Fréquence |
|-------|----------------|-----------|
| Mise à jour météo | Automatisée (tâche planifiée) | Quotidien |
| Mise à jour IFT ADONIS | Data analyst + connaissance Agreste | Tous les 2–3 ans |
| Mise à jour RPG (géométries parcellaires) | Data analyst + géospatial | Annuel |
| Révision calendrier d'épandage | Agronome + data | Tous les 3–5 ans |
| Mise à jour mesures AASQA (validation) | Data analyst | Selon disponibilité |

---

## 7. Infrastructure et coûts

À l'échelle nationale, le volume de données est significatif : ~34 000 communes × 365 jours = environ 12 millions de lignes de risque par an, auxquelles s'ajoutent les données météo (~34 000 centroïdes × 365 jours). Le stockage total est de l'ordre de **5–10 Go**, ce qui reste gérable sur un serveur modeste.

La contrainte principale est la **récupération des données météo via l'API Open-Meteo** : à 100 communes par requête (limite de l'API MétéoFrance), il faut ~340 appels par jour pour couvrir la France entière. Cela prend de l'ordre de 30 à 60 minutes en fonction de la latence réseau. Le calcul de l'indicateur pour une année complète prend plusieurs heures — ce calcul initial est fait une seule fois ; les mises à jour nocturnes ne recalculent que les jours récents.

### Option 1 — VPS (serveur privé virtuel) — recommandé

- **Serveur** : VPS Linux 4 vCPU / 8 Go RAM / 50 Go SSD (~20–35 €/mois selon hébergeur : Scaleway, Hetzner, OVH).
- **Accès** : carte accessible depuis n'importe quel navigateur, y compris mobile.
- **Mise à jour** : tâche cron nocturne automatique.
- **Compétences** : administration Linux basique (SSH, crontab, logs).
- **Limite** : si le service devient très populaire (plusieurs milliers d'utilisateurs simultanés), il faudra migrer vers une infrastructure plus robuste.

### Option 2 — Cloud managé (GCP, AWS, Azure)

- **Avantage** : sauvegardes automatiques, scalabilité à la demande, pas d'administration OS.
- **Inconvénient** : coût plus élevé (~50–150 €/mois), complexité de déploiement initiale plus importante.
- **Recommandation** : pertinent si la carte doit tenir une charge simultanée élevée (> 1 000 utilisateurs) ou si FNE souhaite déléguer totalement l'administration système.

### API météo gratuite (Open-Meteo)

L'API Open-Meteo est **gratuite pour un usage non commercial** dans les conditions actuelles. FNE entre dans cette catégorie. Attention : les conditions peuvent évoluer. Un plan payant (~10–20 €/mois) existe si les volumes augmentent ou si un usage commercial devenait nécessaire.

---

## 8. Estimation du temps de mise en œuvre

### Phase de construction initiale (périmètre national)

| Tâche | Estimation |
|-------|-----------|
| Préparation des données : IFT ADONIS, calendrier épandage, géométries ~34 000 communes | 5–8 jours |
| Développement du calcul de l'indicateur (ETL Python) | 5 jours |
| Récupération météo initiale (historique année en cours) — long à cause du volume API | 3 jours |
| Mise en place de la mise à jour météo nocturne + monitoring | 2–3 jours |
| Optimisation base de données (indexation, partitionnement par année) | 2–3 jours |
| Développement du frontend carte (MapLibre GL + tuiles vectorielles + appels API) | 5–8 jours |
| Déploiement sur VPS | 2 jours |
| Validation des seuils avec experts agronomiques et qualité de l'air | selon interlocuteurs |
| Tests + corrections sur panel de communes | 3–5 jours |
| Documentation + charte de communication | 3–5 jours |
| Consultation juridique | selon interlocuteurs |
| **Total** | **6–10 semaines** (1 personne technique) |

### Maintenance annuelle récurrente

| Tâche | Estimation annuelle |
|-------|---------------------|
| Surveillance tâche nocturne + correction anomalies | 2–3 jours |
| Mise à jour données sources (IFT, RPG si disponible) | 3–5 jours |
| Évolutions mineures de l'interface | selon besoins |
| **Total** | **7–10 jours/an** minimum |

---

## 9. Scénarios de mise en œuvre

### Scénario A — Outil interne national (sans carte publique)

**Pour qui** : chargés de mission FNE pour produire des analyses territoriales, alimenter des plaidoyers, préparer des rapports.
**Infrastructure** : poste de travail ou VPS minimal.
**Ce qu'il faut** : un profil data Python, accès aux données ADONIS et au calendrier FNE.
**Délai** : 6–8 semaines.
**Risque** : faible. Permet de valider la méthodologie et les seuils avant d'investir dans la carte publique.

### Scénario B — Carte publique nationale accessible sur mobile

**Pour qui** : grand public, riverains, journalistes, membres FNE en déplacement sur le terrain.
**Infrastructure** : VPS Linux (~20–35 €/mois) + tâche cron nocturne + frontend MapLibre + tuiles vectorielles.
**Ce qu'il faut** : en plus du Scénario A, un profil web pour le frontend ; validation des seuils par experts ; avis juridique ; charte de communication.
**Délai** : 6–10 semaines (construction) + 1–3 mois (validation et communication).
**Risque** : moyen à élevé sans accompagnement scientifique et juridique préalable.

### Scénario C — Carte nationale avec fonctionnalités avancées

**Pour qui** : grand public avec alertes personnalisées, export de données, comparaison de territoires, intégration dans d'autres outils FNE.
**Infrastructure** : cloud managé (~50–150 €/mois), gestion de charge, authentification utilisateurs.
**Prérequis** : Scénario B validé, comité scientifique, ressources humaines dédiées, financement pérenne.
**Délai** : 6–12 mois supplémentaires.
**Risque** : élevé sans partenariats et financement dédiés.
**Recommandation** : ne pas envisager ce scénario avant que le Scénario B soit stabilisé et utilisé.

---

## 10. Recommandations par ordre de priorité

1. **Commencer par le Scénario A** : calculer et valider l'indicateur à l'échelle nationale en usage interne avant d'investir dans la carte publique. Cela permet de tester la méthodologie sur la diversité des territoires français et d'identifier les ajustements nécessaires.

2. **Fixer les valeurs seuils absolues en concertation avec des experts** avant toute communication externe (agronomistes pour les seuils IFT, spécialistes de la qualité de l'air pour les seuils météo). C'est le prérequis scientifique fondamental.

3. **Mettre en place le monitoring de la mise à jour nocturne dès le départ** : une alerte email en cas d'échec est simple à implémenter et évite de diffuser des données périmées sans le savoir.

4. **Constituer un comité de validation scientifique** (agronome, INRAE ou ANSES, épidémiologiste) avant diffusion publique.

5. **Documenter les limites de façon systématique** dans tout support de communication. L'indicateur est un proxy du risque potentiel, pas une mesure directe d'exposition.

6. **Prévoir la continuité des compétences** : ne pas laisser l'outil dépendre d'une seule personne. Former une deuxième personne ou contractualiser un prestataire pour la maintenance.

---

## 11. Synthèse

| Critère | Évaluation |
|---------|-----------|
| Faisabilité technique (échelle nationale) | Bonne — méthodologie documentée, données accessibles, volume gérable |
| Faisabilité pour carte publique mobile | Conditionnée au choix d'une architecture adaptée (tuiles vectorielles) |
| Maturité pour diffusion publique | Insuffisante sans validation des seuils et avis juridique |
| Facilité de prise en main | Moyenne (nécessite un profil technique dédié) |
| Robustesse à long terme | Faible sans monitoring et maintenance active |
| Coût de mise en production | Faible à moyen (15–30 €/mois + temps humain) |
| Risque de sur-interprétation | Élevé si seuils relatifs et communication mal cadrée |
| Valeur pour le plaidoyer FNE | Réelle si les limites sont bien documentées et les seuils validés |

**Conclusion** : la mise en œuvre de PestiExpo par FNE à l'échelle nationale est faisable avec des ressources raisonnables (6–10 semaines de développement, un profil technique, ~20–35 €/mois de serveur). L'enjeu prioritaire n'est pas technique mais méthodologique : les valeurs seuils de l'indicateur doivent être fixées avec des experts avant toute diffusion, et une charte de communication doit encadrer l'usage des résultats. La carte publique mobile (Scénario B) est atteignable, sous réserve d'un accompagnement juridique et d'un investissement dans une architecture cartographique adaptée aux contraintes du mobile (tuiles vectorielles).

---

*Document rédigé dans le cadre de la phase de prototypage du projet PestiExpo. Les estimations sont indicatives et basées sur l'état de la réflexion en avril 2026.*
