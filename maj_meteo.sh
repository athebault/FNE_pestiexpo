#!/bin/bash
# ============================================================
# PestiExpo — Mise à jour des données météo
#
# Pipeline en 3 couches (géré automatiquement par etl_meteo.py) :
#   1. ERA5 local (Docker) : données historiques depuis le 1er janvier
#   2. Archive distante    : comble le gap entre ERA5 et hier
#   3. Prévisions          : 7 jours via MétéoFrance / Open-Meteo
#
# Usage :
#   ./maj_meteo.sh                              # ERA5 local + gap + prévisions
#   ./maj_meteo.sh --distant                    # Archives distantes + prévisions (sans Docker)
#   ./maj_meteo.sh --region "Pays de la Loire"  # Filtrer sur une région
#   ./maj_meteo.sh --annee 2024                 # Autre année
#   ./maj_meteo.sh --risque                     # Recalcule aussi l'indicateur de risque
#   ./maj_meteo.sh --test                       # Test sur 10 communes (mode local)
#
# Prérequis mode local (Docker ERA5) :
#   docker run -d --rm -v open-meteo-data:/app/data -p 8080:8080 ghcr.io/open-meteo/open-meteo
# ============================================================

set -e

# ── Paramètres par défaut ─────────────────────────────────────
ANNEE=$(date +%Y)
REGION=""
RECALCUL_RISQUE=false
DISTANT=false
TEST=false

# ── Parsing des arguments ─────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case $1 in
        --annee)   ANNEE="$2";           shift 2 ;;
        --region)  REGION="$2";          shift 2 ;;
        --risque)  RECALCUL_RISQUE=true; shift ;;
        --distant) DISTANT=true;         shift ;;
        --test)    TEST=true;            shift ;;
        *)
            echo "Option inconnue : $1"
            echo "Usage : ./maj_meteo.sh [--annee YYYY] [--region NOM] [--distant] [--risque] [--test]"
            exit 1 ;;
    esac
done

# ── Répertoire du projet ──────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

NB_ETAPES=$([ "$RECALCUL_RISQUE" = true ] && echo 2 || echo 1)

echo "============================================================"
echo "  PestiExpo — Mise à jour météo"
echo "  Année     : $ANNEE"
echo "  Région    : ${REGION:-toutes}"
echo "  Mode      : $([ "$DISTANT" = true ] && echo 'Distant' || echo 'Local — Docker ERA5 (défaut)')"
echo "  Date/heure: $(date '+%Y-%m-%d %H:%M')"
echo "============================================================"
echo ""

# ── 1. Météo (ERA5 + gap + prévisions) ───────────────────────
echo "[1/$NB_ETAPES] Récupération des données météo..."

METEO_ARGS="--annee $ANNEE"
[[ -n "$REGION" ]]     && METEO_ARGS="$METEO_ARGS --region \"$REGION\""
[[ "$DISTANT" = true ]] && METEO_ARGS="$METEO_ARGS --distant"
[[ "$TEST" = true ]]    && METEO_ARGS="$METEO_ARGS --test"

eval uv run python3 etl/etl_meteo.py $METEO_ARGS
echo ""

# ── 2. Recalcul de l'indicateur de risque (optionnel) ─────────
if [ "$RECALCUL_RISQUE" = true ]; then
    echo "[2/$NB_ETAPES] Recalcul de l'indicateur de risque journalier $ANNEE + prévisions..."
    RISQUE_ARGS="--annee $ANNEE --previsions"
    [[ -n "$REGION" ]] && RISQUE_ARGS="$RISQUE_ARGS --region \"$REGION\""
    eval uv run python3 etl/calcul_risque_journalier.py $RISQUE_ARGS
    echo ""
fi

echo "============================================================"
echo "  Mise à jour terminée — $(date '+%Y-%m-%d %H:%M')"
echo "============================================================"
