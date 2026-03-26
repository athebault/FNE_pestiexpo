"""
PestiExpo API — FastAPI
Indicateurs d'exposition aux pesticides par commune.

Lancement local :
    uv run uvicorn api.main:app --reload --port 8000

Documentation interactive : http://localhost:8000/docs
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routers import communes, risque, calendrier, mesures
from api.db import annees_disponibles, communes_ref

app = FastAPI(
    title="PestiExpo API",
    description=(
        "API d'indicateurs journaliers d'exposition aux pesticides par commune.\n\n"
        "Basée sur les IFT ADONIS, le calendrier d'épandage et les données météo."
    ),
    version="0.1.0",
    contact={"name": "ARCOOP / FNE"},
    license_info={"name": "Propriétaire"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(communes.router,   prefix="/communes",   tags=["Communes"])
app.include_router(risque.router,     prefix="/risque",     tags=["Risque"])
app.include_router(calendrier.router, prefix="/calendrier", tags=["Calendrier"])
app.include_router(mesures.router,    prefix="/mesures",    tags=["Mesures"])


@app.get("/", include_in_schema=False)
def root():
    return {"message": "PestiExpo API — voir /docs pour la documentation interactive"}


@app.get("/health", tags=["Système"], summary="État de l'API")
def health():
    annees = annees_disponibles()
    nb_communes = len(communes_ref())
    return {
        "status": "ok",
        "communes_chargees": nb_communes,
        "annees_risque_disponibles": annees,
    }
