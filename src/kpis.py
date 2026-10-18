"""Étape 7 du pipeline : PUBLICATION DES KPIs pour le dashboard (zone Gold -> tableaux de bord).

Les prédictions Gold contiennent une ligne par course : trop gros pour un dashboard.
Spark les résume ici en petits tableaux (quelques dizaines de lignes) :
  - activité et erreur du modèle par heure de la journée,
  - par jour de la semaine,
  - par quartier de départ (borough),
  - par tranche de distance,
  - répartition des erreurs de prédiction.
Le résultat est écrit dans reports/kpis_<mois>.json, que le dashboard Streamlit lit directement.
"""
import json
import os

from pyspark.sql import functions as F

from ingest import DATA_DIR, REPORTS_DIR
from spark_utils import get_spark

# Mêmes valeurs que dans features.py / deploy.py (redéfinies ici pour garder une tâche légère :
# pas besoin de charger MLflow ni Great Expectations juste pour lire la zone Gold)
TARGET = "duration_min"


def gold_path(month: str) -> str:
    year, mm = month.split("-")
    return f"{DATA_DIR}/gold/predictions/year={year}/month={mm}"

DAY_NAMES = {1: "Dim", 2: "Lun", 3: "Mar", 4: "Mer", 5: "Jeu", 6: "Ven", 7: "Sam"}


def _rows(df) -> list:
    """Convertit un petit DataFrame Spark en liste de dictionnaires (JSON)."""
    return [row.asDict() for row in df.collect()]


def _summary_by(df, column: str):
    return (df.groupBy(column)
              .agg(F.count("*").alias("trips"),
                   F.round(F.avg(TARGET), 2).alias("avg_real_min"),
                   F.round(F.avg("predicted_duration_min"), 2).alias("avg_predicted_min"),
                   F.round(F.avg("abs_error_min"), 2).alias("mae_min"),
                   F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"))
              .orderBy(column))


def build_kpis(month: str) -> dict:
    spark = get_spark(f"kpis-{month}")
    gold = spark.read.parquet(gold_path(month)).cache()

    total = gold.agg(
        F.count("*").alias("trips"),
        F.round(F.avg(TARGET), 2).alias("avg_real_min"),
        F.round(F.avg("predicted_duration_min"), 2).alias("avg_predicted_min"),
        F.round(F.avg("abs_error_min"), 3).alias("mae_min"),
        F.round(F.avg("trip_distance"), 2).alias("avg_distance_miles"),
        F.round(100 * F.avg("is_airport"), 2).alias("airport_share_pct"),
        F.max("model_version").alias("model_version"),
    ).first().asDict()

    by_day = _rows(_summary_by(gold, "day_of_week"))
    for row in by_day:
        row["day_name"] = DAY_NAMES.get(row["day_of_week"], str(row["day_of_week"]))

    distance_bucket = (F.when(F.col("trip_distance") < 1, "< 1 mile")
                        .when(F.col("trip_distance") < 3, "1-3 miles")
                        .when(F.col("trip_distance") < 5, "3-5 miles")
                        .when(F.col("trip_distance") < 10, "5-10 miles")
                        .otherwise("> 10 miles"))
    error_bucket = (F.when(F.col("abs_error_min") < 1, "< 1 min")
                     .when(F.col("abs_error_min") < 3, "1-3 min")
                     .when(F.col("abs_error_min") < 5, "3-5 min")
                     .when(F.col("abs_error_min") < 10, "5-10 min")
                     .otherwise("> 10 min"))

    kpis = {
        "month": month,
        "totals": total,
        "by_hour": _rows(_summary_by(gold, "pickup_hour")),
        "by_day": by_day,
        "by_borough": _rows(_summary_by(gold, "pickup_borough").orderBy(F.desc("trips"))),
        "by_distance": _rows(_summary_by(gold.withColumn("distance_bucket", distance_bucket), "distance_bucket")),
        "error_distribution": _rows(
            gold.withColumn("error_bucket", error_bucket)
                .groupBy("error_bucket").agg(F.count("*").alias("trips")).orderBy("error_bucket")),
        "top_routes": _rows(
            gold.groupBy("pickup_borough", "dropoff_borough")
                .agg(F.count("*").alias("trips"), F.round(F.avg(TARGET), 2).alias("avg_real_min"))
                .orderBy(F.desc("trips")).limit(10)),
    }
    gold.unpersist()

    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/kpis_{month}.json", "w", encoding="utf-8") as f:
        json.dump(kpis, f, indent=2, ensure_ascii=False, default=str)

    print(f"[KPIs] {month} : {total['trips']:,} courses résumées pour le dashboard "
          f"(durée moyenne {total['avg_real_min']} min, MAE {total['mae_min']} min)")
    return {"month": month, "trips": total["trips"], "kpis_file": f"{REPORTS_DIR}/kpis_{month}.json"}


if __name__ == "__main__":
    import sys
    build_kpis(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
