"""Downloader for Azure Open Datasets (example)
NOTES:
- This script provides helper functions and instructions to download a few Azure Open Datasets (weather, demographics).
- Some datasets are available as public HTTP links (no auth). Others require Azure account/CLI.
- Run with: python scripts/download_azure_open_datasets.py --dataset weather_daily --out data/raw/azure/
"""
import argparse
import os
import urllib.request
from pathlib import Path

SAMPLE_URLS = {
    # Example: public CSV/Parquet links (replace with authoritative links when you download)
    'weather_daily': 'https://example.com/azure-opendatasets-weather-daily.csv',
    'demographics': 'https://example.com/azure-opendatasets-demographics.csv'
}

def download(url, out_path):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} -> {out_path}")
    urllib.request.urlretrieve(url, str(out_path))
    print("Done.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset', required=True, help='Which dataset to download (weather_daily, demographics)')
    parser.add_argument('--out', default='data/raw/azure', help='Output directory')
    args = parser.parse_args()

    if args.dataset not in SAMPLE_URLS:
        print('Unknown dataset. Options:', ','.join(SAMPLE_URLS.keys()))
        return

    url = SAMPLE_URLS[args.dataset]
    out_file = os.path.join(args.out, f"{args.dataset}.csv")
    print('NOTE: SAMPLE_URLS contain placeholders. Replace with the official Azure Open Datasets URLs or download manually.')
    download(url, out_file)

if __name__ == '__main__':
    main()
