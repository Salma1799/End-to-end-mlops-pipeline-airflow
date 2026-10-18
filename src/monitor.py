"""Étape 6 du pipeline : MONITORING de la dérive des données (data drift) avec Evidently.

Question : les courses du mois ressemblent-elles encore à celles sur lesquelles
le modèle en production a été entraîné ?

- Référence : les données du mois d'entraînement du modèle qui était en production
  au début de ce run (l'"ancien champion").
- Données courantes : les données du mois traité.

Evidently compare la distribution de chaque variable (distance, heure, quartiers, durée...).
Si trop de variables ont changé (ou si la durée elle-même a changé), on lève une ALERTE (dans les logs, le rapport et MLflow).
Le rapport HTML détaillé est enregistré dans reports/ et dans MLflow.
"""
import json
import os

import mlflow
from evidently import Report
from evidently.presets import DataDriftPreset

from deploy import MODEL_NAME, _client
from features import TARGET, silver_path
from ingest import REPORTS_DIR
from spark_utils import get_spark

MONITORING_EXPERIMENT = "taxiflow-monitoring"
SAMPLE_ROWS = int(os.getenv("DRIFT_SAMPLE_ROWS", "20000"))
# Alerte si au moins 25 % des variables surveillées ont dérivé, ou si la durée (la cible) elle-même a dérivé
DRIFT_ALERT_SHARE = float(os.getenv("DRIFT_ALERT_SHARE", "0.25"))

MONITORED_COLUMNS = [
    "trip_distance", "pickup_hour", "day_of_week", "is_airport", "passenger_count",
    "pickup_borough", "dropoff_borough", TARGET,
]


def training_month_of(version) -> str:
    """Retrouve le mois sur lequel une version du modèle a été entraînée (tag du run MLflow)."""
    client = _client()
    run_id = client.get_model_version(MODEL_NAME, str(version)).run_id
    return client.get_run(run_id).data.tags["month"]


def _sample(month: str):
    spark = get_spark(f"monitor-{month}")
    df = spark.read.parquet(silver_path(month)).select(*MONITORED_COLUMNS)
    fraction = min(1.0, SAMPLE_ROWS / max(df.count(), 1))
    return df.sample(fraction=fraction, seed=42).toPandas()


def monitor_drift(month: str, reference_version) -> dict:
    os.makedirs(REPORTS_DIR, exist_ok=True)
    if reference_version is None:
        summary = {"month": month, "status": "no_reference",
                   "message": "Premier modèle en production : pas encore de référence à comparer."}
        with open(f"{REPORTS_DIR}/drift_{month}.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        print(f"[Monitoring] {month} : {summary['message']}")
        return summary

    reference_month = training_month_of(reference_version)
    reference, current = _sample(reference_month), _sample(month)

    # Rapport Evidently : une mesure de dérive par variable
    snapshot = Report([DataDriftPreset()]).run(current, reference)
    html_path = f"{REPORTS_DIR}/drift_{month}.html"
    snapshot.save_html(html_path)

    columns = []
    for metric in snapshot.dict()["metrics"]:
        config = metric.get("config", {})
        if config.get("type", "").endswith("ValueDrift"):
            score, threshold, method = float(metric["value"]), float(config["threshold"]), config["method"]
            # Méthodes "p_value" : dérive si p < seuil. Méthodes "distance" : dérive si distance >= seuil
            drifted = score < threshold if "p_value" in method.lower() else score >= threshold
            columns.append({
                "column": config["column"],
                "method": method,
                "score": round(score, 4),
                "threshold": threshold,
                "drifted": drifted,
            })
    drifted = [c["column"] for c in columns if c["drifted"]]
    share = round(len(drifted) / len(columns), 3) if columns else 0.0
    alert = share >= DRIFT_ALERT_SHARE or TARGET in drifted

    summary = {
        "month": month,
        "status": "alert" if alert else "ok",
        "reference_month": reference_month,
        "reference_model_version": str(reference_version),
        "columns_monitored": len(columns),
        "columns_drifted": drifted,
        "drift_share": share,
        "alert_threshold": DRIFT_ALERT_SHARE,
        "avg_duration_reference": round(float(reference[TARGET].mean()), 2),
        "avg_duration_current": round(float(current[TARGET].mean()), 2),
        "details": sorted(columns, key=lambda c: -c["score"]),
        "html_report": html_path,
    }
    with open(f"{REPORTS_DIR}/drift_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Historique du monitoring dans MLflow (un run par mois, avec le rapport HTML)
    mlflow.set_experiment(MONITORING_EXPERIMENT)
    with mlflow.start_run(run_name=f"drift-{month}"):
        mlflow.set_tags({"month": month, "reference_month": reference_month, "status": summary["status"]})
        mlflow.log_metrics({"drift_share": share, "columns_drifted": len(drifted),
                            **{f"drift_{c['column']}": c["score"] for c in columns}})
        mlflow.log_artifact(html_path)

    for c in summary["details"]:
        print(f"[Monitoring] {'DÉRIVE' if c['drifted'] else 'stable'} {c['column']} "
              f"(score {c['score']} / seuil {c['threshold']})")
    if alert:
        print(f"[Monitoring] ⚠️ ALERTE {month} : {len(drifted)}/{len(columns)} variables ont dérivé "
              f"par rapport à {reference_month} ({drifted}). Ré-entraînement recommandé.")
    else:
        print(f"[Monitoring] ✅ {month} : {len(drifted)}/{len(columns)} variables ont dérivé "
              f"par rapport à {reference_month}. Pas d'alerte.")
    return summary
