import openmeteo_requests  # pip install openmeteo-requests

import requests
import pandas as pd

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 44.93,
    "longitude": 4.89,
    "start_date": "2020-01-01",
    "end_date": "2025-12-31",
    "daily": ["temperature_2m_max", "temperature_2m_min", "precipitation_sum",
              "wind_speed_10m_max", "et0_fao_evapotranspiration"],
    "timezone": "Europe/Paris"
}

r = requests.get(url, params=params)
data = r.json()

df = pd.DataFrame(data["daily"])
df["time"] = pd.to_datetime(df["time"])
df.set_index("time", inplace=True)
print(df.head())