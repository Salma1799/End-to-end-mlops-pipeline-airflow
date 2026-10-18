"""Étape 5 du pipeline : DÉPLOIEMENT (stratégie champion / challenger).

- Le "champion" est le modèle actuellement en production (alias "champion" dans le Model Registry MLflow).
- Le "challenger" est le modèle qui vient d'être entraîné.

1. compare_models : on évalue les 2 modèles sur LE MÊME jeu de test (les courses du mois que
   le challenger n'a jamais vues), pour une comparaison équitable.
2. décision (dans le DAG) : si le challenger est meilleur -> il devient champion, sinon on garde l'ancien.
3. score_batch : le champion calcule les prédictions de tout le mois -> zone Gold (prête pour le métier).
"""
import json
import os

import mlflow
from mlflow import MlflowClient
from mlflow.exceptions import MlflowException
from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from features import TARGET, silver_path
from ingest import DATA_DIR, REPORTS_DIR
from spark_utils import get_spark
from train import MODEL_NAME, SEED, evaluate

CHAMPION_ALIAS = "champion"
# Amélioration minimale exigée pour remplacer le champion (en minutes de MAE)
MIN_IMPROVEMENT = float(os.getenv("MIN_IMPROVEMENT_MINUTES", "0.0"))


def gold_path(month: str) -> str:
    year, mm = month.split("-")
    return f"{DATA_DIR}/gold/predictions/year={year}/month={mm}"


def _client() -> MlflowClient:
    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    return MlflowClient()


def load_model(version: str) -> PipelineModel:
    """Télécharge une version du modèle depuis MLflow et la recharge dans Spark."""
    local_dir = mlflow.artifacts.download_artifacts(f"models:/{MODEL_NAME}/{version}")
    return PipelineModel.load(os.path.join(local_dir, "sparkml"))


def get_champion_version(client: MlflowClient):
    try:
        return client.get_model_version_by_alias(MODEL_NAME, CHAMPION_ALIAS).version
    except MlflowException:
        return None  # aucun modèle en production pour l'instant


def compare_models(month: str, challenger_version) -> dict:
    client = _client()
    challenger_version = str(challenger_version)
    champion_version = get_champion_version(client)

    spark = get_spark(f"compare-{month}")
    # Même découpage (même seed) qu'à l'entraînement -> les 20 % de test que le challenger n'a jamais vus
    _, test = spark.read.parquet(silver_path(month)).randomSplit([0.8, 0.2], seed=SEED)
    test.cache()

    challenger = evaluate(load_model(challenger_version).transform(test))
    champion = evaluate(load_model(champion_version).transform(test)) if champion_version else None
    test.unpersist()

    if champion is None:
        decision, reason = "promote", "Aucun modèle en production : le challenger devient champion."
    elif challenger["mae"] < champion["mae"] - MIN_IMPROVEMENT:
        decision = "promote"
        reason = (f"Le challenger (v{challenger_version}) est meilleur : MAE {challenger['mae']} min "
                  f"contre {champion['mae']} min pour le champion (v{champion_version}).")
    else:
        decision = "keep"
        reason = (f"Le champion (v{champion_version}) reste meilleur ou équivalent : MAE {champion['mae']} min "
                  f"contre {challenger['mae']} min pour le challenger (v{challenger_version}).")

    summary = {
        "month": month,
        "challenger_version": challenger_version,
        "challenger_metrics": challenger,
        "champion_version": champion_version,
        "champion_metrics": champion,
        "decision": decision,
        "reason": reason,
    }
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/deployment_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"[Déploiement] {month} : {reason}")
    return summary


def promote_model(month: str, version) -> None:
    """Le challenger devient le champion (= le modèle en production)."""
    client = _client()
    client.set_registered_model_alias(MODEL_NAME, CHAMPION_ALIAS, str(version))
    client.set_model_version_tag(MODEL_NAME, str(version), "promoted_for_month", month)
    print(f"[Déploiement] Version {version} de {MODEL_NAME} promue '{CHAMPION_ALIAS}' (en production).")


def score_batch(month: str) -> dict:
    """Le modèle en production prédit la durée de toutes les courses du mois -> zone Gold."""
    client = _client()
    version = get_champion_version(client)
    if version is None:
        raise RuntimeError("Aucun modèle champion : impossible de calculer les prédictions.")

    spark = get_spark(f"score-{month}")
    data = spark.read.parquet(silver_path(month))
    predictions = (
        load_model(version).transform(data)
        .withColumn("predicted_duration_min", F.round(F.col("prediction"), 2))
        .withColumn("abs_error_min", F.round(F.abs(F.col("prediction") - F.col(TARGET)), 2))
        .withColumn("model_version", F.lit(int(version)))
        .select("trip_distance", "pickup_hour", "day_of_week", "is_airport", "PULocationID", "DOLocationID",
                "pickup_borough", "dropoff_borough", TARGET, "predicted_duration_min", "abs_error_min",
                "model_version")
    )
    target = gold_path(month)
    predictions.write.mode("overwrite").parquet(target)

    stats = spark.read.parquet(target).agg(
        F.count("*").alias("rows"),
        F.round(F.avg("abs_error_min"), 3).alias("mae"),
        F.round(F.avg(TARGET), 2).alias("avg_real"),
        F.round(F.avg("predicted_duration_min"), 2).alias("avg_predicted"),
    ).first()

    summary = {
        "month": month,
        "model_version": int(version),
        "rows_scored": stats["rows"],
        "mae_on_month": stats["mae"],
        "avg_real_duration": stats["avg_real"],
        "avg_predicted_duration": stats["avg_predicted"],
        "gold_path": target,
    }
    with open(f"{REPORTS_DIR}/gold_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    print(f"[Gold] {month} : {stats['rows']:,} prédictions par le modèle v{version} "
          f"(MAE = {stats['mae']} min, durée réelle moyenne {stats['avg_real']} min, "
          f"prédite {stats['avg_predicted']} min)")
    return summary
