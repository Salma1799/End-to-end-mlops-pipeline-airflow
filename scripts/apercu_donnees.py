"""Aperçu d'un fichier brut NYC TLC (pour la démo) : d'où il vient, sa taille, ses colonnes, 5 courses.

Utilisation (depuis le dossier taxiflow) :
    docker compose exec airflow python /opt/airflow/scripts/apercu_donnees.py 2024-01
"""
import os
import sys

import pyarrow.parquet as pq

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
TLC_BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")
SHOW = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
        "trip_distance", "PULocationID", "DOLocationID", "fare_amount"]

month = sys.argv[1] if len(sys.argv) > 1 else "2024-01"
path = f"{DATA_DIR}/raw/yellow_tripdata_{month}.parquet"
if not os.path.exists(path):
    sys.exit(f"Fichier absent : {path} (la Task 1 du DAG le téléchargera automatiquement)")

meta = pq.ParquetFile(path).metadata
print("=" * 70)
print(f" Source   : {TLC_BASE_URL}/trip-data/yellow_tripdata_{month}.parquet")
print(f" Fichier  : {path}")
print(f" Taille   : {os.path.getsize(path) / 1e6:.1f} Mo (format Parquet)")
print(f" Courses  : {meta.num_rows:,}".replace(",", " "))
print(f" Colonnes : {meta.num_columns}")
print("=" * 70)

first = pq.ParquetFile(path).read_row_group(0, columns=SHOW).slice(0, 5).to_pandas()
print(first.to_string(index=False))
