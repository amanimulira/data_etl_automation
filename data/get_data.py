import pandas as pd
from pathlib import Path

RAW_DIR = Path('data/raw/azure')
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Weather dataset (daily weather)
weather_url = "https://azureopendatastorage.blob.core.windows.net/nyctlc/nyc_weather/weather_daily.csv"
weather_df = pd.read_csv(weather_url)
weather_df.to_csv(RAW_DIR / 'weather_daily.csv', index=False)

# Demographics dataset
demo_url = "https://azureopendatastorage.blob.core.windows.net/nyctlc/nyc_demographics/demographics.csv"
demo_df = pd.read_csv(demo_url)
demo_df.to_csv(RAW_DIR / 'demographics.csv', index=False)

print("Downloaded weather and demographics data.")
