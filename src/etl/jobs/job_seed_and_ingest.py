"""
Simple CLI ETL job: connect to AdventureWorks, read Azure CSVs, clean and merge, write analytics_ready.csv
Usage (from project root): python -m src.etl.jobs.job_seed_and_ingest
"""

import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path

# Relative imports — works only when run as module
from ...connectors.sqlserver_adventureworks import (
    get_engine_from_env,
    get_sales_transactions,
    get_customers,
    get_sales_order_details
)

load_dotenv()

RAW_DIR = Path(os.getenv('RAW_DATA_DIR', 'data/raw'))
OUT_DIR = Path(os.getenv('ANALYTICS_OUTPUT_DIR', 'data/analytics'))
OUT_DIR.mkdir(parents=True, exist_ok=True)


def clean_data(df):
    df = df.drop_duplicates()
    # common replacements
    for col in df.select_dtypes(['object']).columns:
        df[col] = df[col].str.strip()
    if 'OrderDate' in df.columns:
        df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')
    return df


def load_azure_enrichment():
    azure_dir = RAW_DIR / 'azure'
    weather = None
    demo = None
    wpath = azure_dir / 'weather_daily.csv'
    dpath = azure_dir / 'demographics.csv'
    if wpath.exists():
        weather = pd.read_csv(wpath)
    if dpath.exists():
        demo = pd.read_csv(dpath)
    return weather, demo


def run():
    engine = get_engine_from_env()
    print('Loading sales transactions...')
    sales = get_sales_transactions(engine, start_date=None)
    print('Loading order details...')
    details = get_sales_order_details(engine)
    print('Loading customers...')
    customers = get_customers(engine)

    sales = clean_data(sales)
    details = clean_data(details)
    customers = clean_data(customers)

    print('Merging header + details...')
    merged = sales.merge(details, on='SalesOrderID', how='left')
    merged = merged.merge(customers, left_on='CustomerID', right_on='CustomerID', how='left')

    weather, demo = load_azure_enrichment()
    if weather is not None and not weather.empty:
        print("Weather columns:", weather.columns.tolist())
        # Check if required date columns exist
        if all(col in weather.columns for col in ['Year', 'Month', 'Day']):
            # Create a 'date' column from Year, Month, Day
            weather['date'] = pd.to_datetime(weather[['Year', 'Month', 'Day']].astype(str).agg('-'.join, axis=1), 
                                        format='%Y-%m-%d', errors='coerce')
            merged['OrderDate_date'] = pd.to_datetime(merged['OrderDate']).dt.date
            weather['date_only'] = weather['date'].dt.date
            merged = merged.merge(weather, left_on='OrderDate_date', right_on='date_only', how='left')
        else:
            print("Warning: Missing 'Year', 'Month', or 'Day' columns in weather data. Skipping weather merge.")
    else:
        print("Warning: Weather data is empty or not loaded. Skipping weather merge.")

    out_file = OUT_DIR / 'analytics_ready.csv'
    merged.to_csv(out_file, index=False)
    print('Wrote', out_file)


if __name__ == '__main__':
    # Check if run as script incorrectly
    if __package__ is None or __package__ == '':
        raise SystemExit(
            "This script uses relative imports and must be run as a module:\n"
            "python -m src.etl.jobs.job_seed_and_ingest"
        )
    run()
