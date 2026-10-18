"""Fabrique un mois CORROMPU pour la démo : le pipeline doit le bloquer au contrôle qualité.

On part des vraies courses de juillet 2024, on les décale en août (faux mois "2024-08"),
puis on abîme volontairement une partie des données, comme le ferait un fournisseur défaillant :
  - 25 % de prix négatifs,
  - 20 % de distances à 0,
  - 10 % de courses qui se terminent AVANT d'avoir commencé.

Utilisation (depuis le dossier taxiflow) :
    docker compose exec airflow python /opt/airflow/scripts/corrupt_data.py
Puis lancer le DAG avec month = 2024-08 : la tâche validate_raw doit passer au ROUGE.
"""
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DATA_DIR = os.getenv("DATA_DIR", "/opt/airflow/data")
SOURCE = f"{DATA_DIR}/raw/yellow_tripdata_2024-07.parquet"
TARGET = f"{DATA_DIR}/raw/yellow_tripdata_2024-08.parquet"
COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance",
    "RatecodeID", "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "total_amount",
]


def main() -> None:
    # Le vrai mois de juillet doit être présent : la Task 1 sait le télécharger s'il manque
    from ingest import extract_month
    extract_month("2024-07")
    df = pq.read_table(SOURCE, columns=COLUMNS).to_pandas()
    rng = np.random.default_rng(2024)
    n = len(df)

    # Décaler juillet -> août (31 jours) pour obtenir un "nouveau" mois
    for col in ("tpep_pickup_datetime", "tpep_dropoff_datetime"):
        df[col] = df[col] + np.timedelta64(31, "D")

    # Corruptions volontaires
    negative_fare = rng.random(n) < 0.25
    df.loc[negative_fare, "fare_amount"] = -df.loc[negative_fare, "fare_amount"].abs()

    zero_distance = rng.random(n) < 0.20
    df.loc[zero_distance, "trip_distance"] = 0.0

    time_travel = rng.random(n) < 0.10
    df.loc[time_travel, "tpep_dropoff_datetime"] = (
        df.loc[time_travel, "tpep_pickup_datetime"] - np.timedelta64(10, "m"))

    pq.write_table(pa.Table.from_pandas(df, preserve_index=False), TARGET)
    print(f"Mois corrompu créé : {TARGET} ({n:,} courses)")
    print(f"  - prix négatifs        : {negative_fare.sum():,} ({negative_fare.mean():.0%})")
    print(f"  - distances nulles     : {zero_distance.sum():,} ({zero_distance.mean():.0%})")
    print(f"  - arrivée avant départ : {time_travel.sum():,} ({time_travel.mean():.0%})")
    print("Lance maintenant le DAG avec month = 2024-08 : validate_raw doit passer au ROUGE.")


if __name__ == "__main__":
    main()
