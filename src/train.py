"""Étape 4 du pipeline : ENTRAÎNEMENT (Spark MLlib) + SUIVI DES EXPÉRIENCES (MLflow).

Pour le mois choisi :
1. on lit les données Silver et on les coupe en 80 % entraînement / 20 % test,
2. on entraîne 2 modèles : une régression linéaire (modèle de référence simple)
   et un Gradient Boosted Trees (GBT, plus puissant),
3. on mesure l'erreur de chaque modèle sur le jeu de test (MAE, RMSE, R²),
4. on enregistre tout dans MLflow (paramètres, métriques, modèle),
5. le meilleur modèle est inscrit dans le Model Registry de MLflow (nouvelle version).
   La décision de le mettre en production se fera à l'étape suivante.
"""
import json
import os
import time

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression

from features import TARGET, silver_path
from ingest import REPORTS_DIR
from spark_utils import get_spark

EXPERIMENT_NAME = "taxiflow-duration"
MODEL_NAME = "taxiflow-duration-model"
SEED = 42

NUMERIC_FEATURES = [
    "trip_distance", "pickup_hour", "day_of_week", "is_weekend", "is_airport", "same_zone",
    "passenger_count", "RatecodeID", "PULocationID", "DOLocationID",
]
CATEGORICAL_FEATURES = ["pickup_borough", "dropoff_borough"]


def build_pipeline(model) -> Pipeline:
    """Préparation commune : texte -> nombres (StringIndexer), puis toutes les variables dans un vecteur."""
    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL_FEATURES]
    assembler = VectorAssembler(
        inputCols=NUMERIC_FEATURES + [f"{c}_idx" for c in CATEGORICAL_FEATURES], outputCol="features")
    return Pipeline(stages=indexers + [assembler, model])


def candidate_models() -> dict:
    return {
        "linear_regression": (LinearRegression(featuresCol="features", labelCol=TARGET, maxIter=50, regParam=0.1),
                              {"maxIter": 50, "regParam": 0.1}),
        "gbt": (GBTRegressor(featuresCol="features", labelCol=TARGET, maxIter=30, maxDepth=6, seed=SEED),
                {"maxIter": 30, "maxDepth": 6}),
    }


def evaluate(predictions) -> dict:
    metrics = {}
    for name in ("mae", "rmse", "r2"):
        evaluator = RegressionEvaluator(labelCol=TARGET, predictionCol="prediction", metricName=name)
        metrics[name] = round(evaluator.evaluate(predictions), 4)
    return metrics


def train_month(month: str) -> dict:
    spark = get_spark(f"train-{month}")
    data = spark.read.parquet(silver_path(month))
    # Part des données Silver utilisée pour l'entraînement (0.3 = 30 %) : évite le manque de mémoire sur 8 Go
    sample = float(os.getenv("TRAIN_SAMPLE_FRACTION", "1.0"))
    if sample < 1.0:
        data = data.sample(fraction=sample, seed=SEED)
    train, test = data.randomSplit([0.8, 0.2], seed=SEED)
    train.cache()
    test.cache()
    n_train, n_test = train.count(), test.count()

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(EXPERIMENT_NAME)

    results = {}
    for name, (model, params) in candidate_models().items():
        with mlflow.start_run(run_name=f"{month}-{name}") as run:
            start = time.time()
            fitted = build_pipeline(model).fit(train)
            metrics = evaluate(fitted.transform(test))
            metrics["training_seconds"] = round(time.time() - start, 1)

            mlflow.set_tags({"month": month, "model_type": name})
            mlflow.log_params({**params, "month": month, "rows_train": n_train, "rows_test": n_test,
                               "features": ", ".join(NUMERIC_FEATURES + CATEGORICAL_FEATURES)})
            mlflow.log_metrics(metrics)

            # Importance des variables (uniquement pour le GBT) : quelles variables comptent le plus ?
            if name == "gbt":
                names = NUMERIC_FEATURES + CATEGORICAL_FEATURES
                importances = fitted.stages[-1].featureImportances.toArray()
                ranking = sorted(zip(names, importances), key=lambda x: -x[1])
                mlflow.log_dict({n: round(float(v), 4) for n, v in ranking}, "feature_importances.json")

            model_info = mlflow.spark.log_model(fitted, artifact_path="model")
            results[name] = {"run_id": run.info.run_id, "model_uri": model_info.model_uri, **metrics}
            print(f"[Entraînement] {month} {name} : MAE = {metrics['mae']} min, "
                  f"RMSE = {metrics['rmse']} min, R² = {metrics['r2']} ({metrics['training_seconds']} s)")

    train.unpersist()
    test.unpersist()

    # Le meilleur modèle = la plus petite erreur moyenne (MAE) -> nouvelle version dans le Model Registry
    best_name = min(results, key=lambda k: results[k]["mae"])
    best = results[best_name]
    version = mlflow.register_model(best["model_uri"], MODEL_NAME)

    summary = {
        "month": month,
        "rows_train": n_train,
        "rows_test": n_test,
        "models": {k: {m: v[m] for m in ("mae", "rmse", "r2", "training_seconds")} for k, v in results.items()},
        "best_model": best_name,
        "best_run_id": best["run_id"],
        "registered_model": MODEL_NAME,
        "registered_version": int(version.version),
    }
    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/training_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    print(f"[Entraînement] Meilleur modèle : {best_name} (MAE = {best['mae']} min) "
          f"-> {MODEL_NAME} version {version.version}")
    return summary


if __name__ == "__main__":
    import sys
    train_month(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
