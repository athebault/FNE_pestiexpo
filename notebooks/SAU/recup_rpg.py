import requests
import pandas as pd

# Lister les ressources RPG disponibles via l'API data.gouv.fr
DATASET_ID = "62f764b5b0e5571f89793ba9"  # ID du jeu RPG sur data.gouv.fr
url = f"https://www.data.gouv.fr/api/1/datasets/{DATASET_ID}/"
r = requests.get(url)
data = r.json()

# Lister toutes les ressources (une par région)
for resource in data["resources"]:
    print(resource["title"], "→", resource["url"])