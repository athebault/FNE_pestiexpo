import requests
from bs4 import BeautifulSoup
import base64
from datetime import datetime
import io

# --- Config ---
API_KEY_RAW = "eyJ4NXQiOiJOelU0WTJJME9XRXhZVGt6WkdJM1kySTFaakZqWVRJeE4yUTNNalEyTkRRM09HRmtZalkzTURkbE9UZ3paakUxTURRNFltSTVPR1kyTURjMVkyWTBNdyIsImtpZCI6Ik56VTRZMkkwT1dFeFlUa3paR0kzWTJJMVpqRmpZVEl4TjJRM01qUTJORFEzT0dGa1lqWTNNRGRsT1RnelpqRTFNRFE0WW1JNU9HWTJNRGMxWTJZME13X1JTMjU2IiwidHlwIjoiYXQrand0IiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiJkYWZjZDZhYS02ZTA4LTRiZDQtODFhYy1kN2MyNDBlYWNlYTUiLCJhdXQiOiJBUFBMSUNBVElPTiIsImF1ZCI6InYxelM2ZnFiN0xKZmM2bnVLelhvWndlb3lRVWEiLCJuYmYiOjE3NzE2MTA3MjAsImF6cCI6InYxelM2ZnFiN0xKZmM2bnVLelhvWndlb3lRVWEiLCJzY29wZSI6ImRlZmF1bHQiLCJpc3MiOiJodHRwczpcL1wvcG9ydGFpbC1hcGkubWV0ZW9mcmFuY2UuZnJcL29hdXRoMlwvdG9rZW4iLCJleHAiOjE3NzE2MTQzMjAsImlhdCI6MTc3MTYxMDcyMCwianRpIjoiOWM3NmMyZDUtODMxNC00MTNhLWIyMDMtMjc2ZDBkYjk3OWRkIiwiY2xpZW50X2lkIjoidjF6UzZmcWI3TEpmYzZudUt6WG9ad2VveVFVYSJ9.yHPI8UZNhRvmC4i8TcRgmKT1om_zDb8qoO9sayRhC40-foumv0VSJN5a8aykjArzzb452z2OdZuUMGR9dmPDIB8LTPyVTdVzRsxCoxJDIga8A8PnC4rkF-xCD1PtCEngJFnrteyCFO_RfZCcrbsrEdMhofzbcI52eYk1FVUAVN_EZ54STpibKpUeM3Z8N4qkg_Bs9Dc30f-LPSXvIx42w8uWBWIVNACp0HJT_WwcA6Xhqupk9JUzdYEospwIllIe7KxzwqpMfzXhL8-V64kEDqib5zY21MptfK5H8OLAQ9sNCO4D7KdpQ3Wf63wYqmKAR9Abg5kVuVS9QFogPdrjJA"
# Encodage base64 de la clé
API_KEY = base64.b64encode(API_KEY_RAW.encode()).decode()

BASE_URL = "https://public-api.meteofrance.fr/public"
RESOURCE = "/arpege/1.0/wcs/MF-NWP-GLOBAL-ARPEGE-01-EUROPE-WCS"

headers = {"apikey": API_KEY}

# --- 1. Récupérer les capabilities (runs disponibles) ---
caps_url = f"{BASE_URL}{RESOURCE}/GetCapabilities?service=WCS&version=2.0.1&language=fre"
response = requests.get(caps_url, headers=headers)
soup = BeautifulSoup(response.content, "xml")

# Lister les layers disponibles
layers = soup.find_all("wcs:Identifier")
for layer in layers[:10]:
    print(layer.text)

# --- 2. Sélectionner la variable température à 2m ---
TARGET_LAYER = "TEMPERATURE__SPECIFIC_HEIGHT_LEVEL_ABOVE_GROUND"

layer_tag = soup.find("wcs:Identifier", string=TARGET_LAYER)
times = layer_tag.find_next("gml:TimePosition")
# Récupérer le dernier run disponible
all_times = layer_tag.find_all_next("gml:TimePosition")
reference_time = all_times[-1].text
print(f"Dernier run : {reference_time}")

# --- 3. Télécharger les données (GetCoverage) ---
# Zone Europe (ou adapter les coordonnées)
lat_min, lat_max = 41.0, 51.5
lon_min, lon_max = -5.0, 10.0
forecast_time = reference_time  # ou choisir un horizon

coverage_url = (
    f"{BASE_URL}{RESOURCE}/GetCoverage"
    f"?service=WCS&version=2.0.1"
    f"&coverageId={TARGET_LAYER}__ground_or_water_surface"
    f"&subset=time({forecast_time})"
    f"&subset=lat({lat_min},{lat_max})"
    f"&subset=long({lon_min},{lon_max})"
    f"&format=application/wmo-grib2"
)

r = requests.get(coverage_url, headers=headers)

with open("arpege_t2m.grib2", "wb") as f:
    f.write(r.content)
print("Fichier GRIB2 téléchargé.")