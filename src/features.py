"""Étape 3 du pipeline : PRÉPARATION DES DONNÉES (zone Silver).

À partir des données Bronze validées :
1. on écarte les courses aberrantes (mêmes règles que le contrôle qualité),
2. on récupère les courses sans nombre de passagers (valeur vide -> 1, le cas le plus fréquent),
3. on crée les variables (features) qui aideront le modèle à prédire la durée,
4. on enregistre le résultat en zone Silver.

Attention à la fuite de données : le prix (fare_amount, total_amount) est calculé APRÈS la course,
il "connaît" déjà la durée. On ne le garde donc PAS comme variable pour le modèle.
"""
import json
import os

from pyspark.sql import functions as F

from ingest import DATA_DIR, REPORTS_DIR, bronze_path
from spark_utils import get_spark
from validate import add_duration, month_bounds

ZONES_PATH = f"{DATA_DIR}/raw/taxi_zone_lookup.csv"
AIRPORT_ZONES = [1, 132, 138]  # Newark (EWR), JFK, LaGuardia

# Colonnes finales de la zone Silver : les variables du modèle + la cible
FEATURE_COLUMNS = [
    "trip_distance", "pickup_hour", "day_of_week", "is_weekend", "is_airport", "same_zone",
    "passenger_count", "RatecodeID", "PULocationID", "DOLocationID", "pickup_borough", "dropoff_borough",
]
TARGET = "duration_min"


def silver_path(month: str) -> str:
    year, mm = month.split("-")
    return f"{DATA_DIR}/silver/yellow/year={year}/month={mm}"


def build_features(month: str) -> dict:
    spark = get_spark(f"features-{month}")
    df = add_duration(spark.read.parquet(bronze_path(month)))
    rows_in = df.count()

    # 1. Récupérer les courses sans nombre de passagers (ou à 0) : on met 1, le cas le plus fréquent
    missing_passengers = df.filter(F.col("passenger_count").isNull() | (F.col("passenger_count") == 0)).count()
    df = df.withColumn(
        "passenger_count",
        F.when(F.col("passenger_count").isNull() | (F.col("passenger_count") == 0), 1).otherwise(F.col("passenger_count")),
    ).withColumn("RatecodeID", F.coalesce(F.col("RatecodeID"), F.lit(99)))  # 99 = tarif inconnu

    # 2. Écarter les courses aberrantes (mêmes seuils que le contrôle qualité)
    start, end = month_bounds(month)
    df = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(start)) & (F.col("tpep_pickup_datetime") < F.lit(end))
        & F.col("duration_min").between(1, 180)
        & F.col("trip_distance").between(0.1, 100)
        & F.col("passenger_count").between(1, 6)
        & F.col("fare_amount").between(0, 1000)
        & F.col("PULocationID").between(1, 265)
        & F.col("DOLocationID").between(1, 265)
    )

    # 3. Variables temporelles : l'heure et le jour influencent fortement le trafic
    df = (
        df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
          .withColumn("day_of_week", F.dayofweek("tpep_pickup_datetime"))  # 1 = dimanche ... 7 = samedi
          .withColumn("is_weekend", F.col("day_of_week").isin(1, 7).cast("int"))
          .withColumn("is_airport", (F.col("PULocationID").isin(AIRPORT_ZONES)
                                     | F.col("DOLocationID").isin(AIRPORT_ZONES)
                                     | (F.col("RatecodeID") == 2)).cast("int"))
          .withColumn("same_zone", (F.col("PULocationID") == F.col("DOLocationID")).cast("int"))
    )

    # 4. Quartier de départ et d'arrivée (Manhattan, Queens...) grâce à la table des zones
    zones = spark.read.option("header", True).csv(ZONES_PATH).select(
        F.col("LocationID").cast("int").alias("LocationID"), F.col("Borough").alias("borough"))
    df = (
        df.join(F.broadcast(zones.withColumnRenamed("LocationID", "PULocationID")
                                 .withColumnRenamed("borough", "pickup_borough")), "PULocationID", "left")
          .join(F.broadcast(zones.withColumnRenamed("LocationID", "DOLocationID")
                                 .withColumnRenamed("borough", "dropoff_borough")), "DOLocationID", "left")
          .fillna({"pickup_borough": "Unknown", "dropoff_borough": "Unknown"})
    )

    # 5. Écriture en zone Silver (uniquement les variables + la cible : pas le prix !)
    silver = df.select(*FEATURE_COLUMNS, TARGET)
    target = silver_path(month)
    silver.write.mode("overwrite").parquet(target)

    saved = spark.read.parquet(target)
    stats = saved.agg(
        F.count("*").alias("rows"),
        F.round(F.avg(TARGET), 2).alias("avg_duration"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance"),
    ).first()

    summary = {
        "month": month,
        "rows_bronze": rows_in,
        "passengers_imputed": missing_passengers,
        "rows_silver": stats["rows"],
        "kept_percent": round(100 * stats["rows"] / rows_in, 2) if rows_in else 0.0,
        "avg_duration_min": stats["avg_duration"],
        "avg_distance_miles": stats["avg_distance"],
        "silver_path": target,
    }
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/features_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"[Features] {month} : {rows_in:,} courses Bronze -> {stats['rows']:,} en Silver "
          f"({summary['kept_percent']} %), dont {missing_passengers:,} passagers complétés. "
          f"Durée moyenne : {stats['avg_duration']} min")
    return summary


if __name__ == "__main__":
    import sys
    build_features(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
