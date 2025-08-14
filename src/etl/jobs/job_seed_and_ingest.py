"""Simple CLI ETL job: connect to AdventureWorks, read Azure CSVs, clean and merge, write analytics_ready.csv
Usage: python src/etl/jobs/job_seed_and_ingest.py
"""
import os
from dotenv import load_dotenv
import pandas as pd
from pathlib import Path
from src.connectors.sqlserver_adventureworks import get_engine_from_env, get_sales_transactions, get_customers, get_sales_order_details

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
    # load weather / demographics if available
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
    if weather is not None:
        # example join: merge on OrderDate -> date and region (if available)
        weather['date'] = pd.to_datetime(weather['date'], errors='coerce')
        merged['OrderDate_date'] = pd.to_datetime(merged['OrderDate']).dt.date
        weather['date_only'] = weather['date'].dt.date
        merged = merged.merge(weather, left_on='OrderDate_date', right_on='date_only', how='left')

    out_file = OUT_DIR / 'analytics_ready.csv'
    merged.to_csv(out_file, index=False)
    print('Wrote', out_file)

if __name__ == '__main__':
    run()
