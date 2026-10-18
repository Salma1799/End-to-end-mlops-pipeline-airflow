"""Étape 1 du pipeline : EXTRACTION + INGESTION.

1. Extraction : si le fichier du mois n'est pas encore dans data/raw, il est téléchargé
   automatiquement depuis l'open data officiel de la NYC TLC (Taxi & Limousine Commission).
2. Ingestion : le fichier brut est rangé dans la zone Bronze du data lake, sans le nettoyer,
   en ajoutant des colonnes de traçabilité (date d'ingestion, fichier source).

Bronze = les données telles qu'elles sont arrivées. Le nettoyage se fera plus tard (zone Silver).
"""
import json
import os
import shutil
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import functions as F

from spark_utils import get_spark

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
REPORTS_DIR = os.getenv("REPORTS_DIR", "/opt/airflow/reports")
# Part des courses gardées (0.15 = 15 %), pour que le projet tourne sur un PC de 8 Go
SAMPLE_FRACTION = float(os.getenv("SAMPLE_FRACTION", "0.15"))

# Adresse officielle de l'open data NYC TLC (un fichier Parquet par mois)
TLC_BASE_URL = os.getenv("TLC_BASE_URL", "https://d37ci6vzurychx.cloudfront.net")
TRIPS_URL = TLC_BASE_URL + "/trip-data/yellow_tripdata_{month}.parquet"
ZONES_URL = TLC_BASE_URL + "/misc/taxi_zone_lookup.csv"

# Colonnes utiles au projet, avec un type fixe pour que tous les mois aient le même schéma
COLUMNS = {
    "VendorID": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "RatecodeID": "int",
    "store_and_fwd_flag": "string",
    "PULocationID": "int",
    "DOLocationID": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "total_amount": "double",
}


def raw_path(month: str) -> str:
    return f"{DATA_DIR}/raw/yellow_tripdata_{month}.parquet"


def download(url: str, target: str) -> None:
    """Télécharge un fichier dans un fichier temporaire puis le renomme :
    un téléchargement coupé en plein milieu ne laisse jamais un fichier abîmé dans data/raw."""
    os.makedirs(os.path.dirname(target), exist_ok=True)
    tmp = target + ".part"
    print(f"[Extraction] Téléchargement de {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "taxiflow-student-project"})
    try:
        with urllib.request.urlopen(request, timeout=120) as response, open(tmp, "wb") as f:
            shutil.copyfileobj(response, f)
        os.replace(tmp, target)
    finally:
        if os.path.exists(tmp):
            os.remove(tmp)
    print(f"[Extraction] {os.path.getsize(target) / 1e6:.1f} Mo enregistrés dans {target}")


def extract_month(month: str) -> str:
    """Étape d'extraction : garantit que le fichier du mois (et la table des zones) sont dans data/raw.
    Un fichier déjà présent n'est jamais retéléchargé (c'est le cas du mois corrompu de la démo)."""
    source = raw_path(month)
    if not os.path.exists(source):
        try:
            download(TRIPS_URL.format(month=month), source)
        except Exception as error:
            raise FileNotFoundError(
                f"Fichier introuvable : {source}, et le téléchargement depuis la NYC TLC a échoué ({error}). "
                f"Vérifie la connexion Internet, ou télécharge le fichier à la main dans data/raw."
            ) from error
    zones = f"{DATA_DIR}/raw/taxi_zone_lookup.csv"
    if not os.path.exists(zones):
        download(ZONES_URL, zones)
    return source


def bronze_path(month: str) -> str:
    year, mm = month.split("-")
    return f"{DATA_DIR}/bronze/yellow/year={year}/month={mm}"


def ingest_month(month: str, sample_fraction: float = SAMPLE_FRACTION) -> dict:
    """Extrait puis ingère un mois (format 'AAAA-MM') : open data NYC TLC -> data/raw -> zone Bronze."""
    source = extract_month(month)

    spark = get_spark(f"ingest-{month}")
    raw = spark.read.parquet(source)

    # 1. Garder les colonnes utiles avec un type homogène
    df = raw.select([F.col(c).cast(t).alias(c) for c, t in COLUMNS.items()])
    rows_raw = df.count()

    # 2. Échantillonnage reproductible (seed fixe = même échantillon à chaque exécution)
    if sample_fraction < 1.0:
        df = df.sample(fraction=sample_fraction, seed=42)

    # 3. Colonnes de traçabilité
    df = df.withColumn("ingested_at", F.lit(datetime.now(timezone.utc).isoformat())) \
           .withColumn("source_file", F.lit(os.path.basename(source)))

    # 4. Écriture en Bronze (on remplace le mois s'il a déjà été ingéré : relancer ne crée pas de doublons)
    target = bronze_path(month)
    df.write.mode("overwrite").parquet(target)
    rows_bronze = spark.read.parquet(target).count()

    summary = {
        "month": month,
        "source_file": os.path.basename(source),
        "rows_raw": rows_raw,
        "sample_fraction": sample_fraction,
        "rows_bronze": rows_bronze,
        "bronze_path": target,
    }

    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/ingestion_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"[Ingestion] {month} : {rows_raw:,} courses brutes -> {rows_bronze:,} en Bronze ({target})")
    return summary


if __name__ == "__main__":
    import sys
    ingest_month(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
