"""DAG principal de TaxiFlow.

On le lance à la main en choisissant le mois à traiter (ex. 2024-01).
Étapes en place :
ingestion -> contrôle qualité -> préparation des données -> entraînement (MLflow)
-> comparaison champion/challenger -> décision -> promotion (ou non) -> prédictions Gold
-> monitoring de la dérive (Evidently) -> publication des KPIs pour le dashboard (Streamlit)

Traçabilité : chaque tâche déclare ce qu'elle lit (inlets) et écrit (outlets).
OpenLineage les envoie à Marquez (http://localhost:3000), qui dessine le graphe du parcours des données.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

from lineage import datasets  # jeux de données déclarés pour la traçabilité (OpenLineage)


def _ingest(**context):
    from ingest import ingest_month
    return ingest_month(context["params"]["month"])


def _validate(**context):
    from validate import validate_month
    return validate_month(context["params"]["month"])  # lève une erreur si la qualité est insuffisante


def _features(**context):
    from features import build_features
    return build_features(context["params"]["month"])


def _train(**context):
    from train import train_month
    return train_month(context["params"]["month"])


def _compare(**context):
    from deploy import compare_models
    trained = context["ti"].xcom_pull(task_ids="train_model")  # résultat de l'étape d'entraînement
    return compare_models(context["params"]["month"], trained["registered_version"])


def _decide(**context):
    """Choisit la branche suivante du DAG selon le résultat de la comparaison."""
    comparison = context["ti"].xcom_pull(task_ids="compare_models")
    return "promote_model" if comparison["decision"] == "promote" else "keep_champion"


def _promote(**context):
    from deploy import promote_model
    comparison = context["ti"].xcom_pull(task_ids="compare_models")
    promote_model(context["params"]["month"], comparison["challenger_version"])


def _score(**context):
    from deploy import score_batch
    return score_batch(context["params"]["month"])


def _monitor(**context):
    from monitor import monitor_drift
    comparison = context["ti"].xcom_pull(task_ids="compare_models")
    # Référence = le modèle qui était en production au début de ce run (l'ancien champion)
    return monitor_drift(context["params"]["month"], comparison["champion_version"])


def _kpis(**context):
    from kpis import build_kpis
    return build_kpis(context["params"]["month"])


with DAG(
    dag_id="taxiflow_pipeline",
    description="Pipeline MLOps TaxiFlow : ingestion -> qualité -> modèle -> déploiement -> monitoring",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # lancement manuel, en choisissant le mois
    catchup=False,
    max_active_runs=1,      # un seul mois traité à la fois (économie de RAM)
    params={
        "month": Param(
            "2024-01",
            type="string",
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Mois à traiter, au format AAAA-MM (ex. 2024-01)",
        )
    },
    tags=["taxiflow"],
) as dag:

    # inlets = ce que la tâche lit, outlets = ce qu'elle écrit (utilisés par OpenLineage / Marquez)
    ingest = PythonOperator(task_id="ingest_month", python_callable=_ingest,
                            inlets=datasets("tlc_open_data"), outlets=datasets("bronze", "taxi_zones"))
    validate = PythonOperator(task_id="validate_raw", python_callable=_validate,
                              inlets=datasets("bronze"), outlets=datasets("quality"))
    features = PythonOperator(task_id="build_features", python_callable=_features,
                              inlets=datasets("bronze", "taxi_zones"), outlets=datasets("silver"))
    train = PythonOperator(task_id="train_model", python_callable=_train,
                           inlets=datasets("silver"), outlets=datasets("model"))
    compare = PythonOperator(task_id="compare_models", python_callable=_compare,
                             inlets=datasets("silver", "model"))
    decide = BranchPythonOperator(task_id="decide_deploy", python_callable=_decide)
    promote = PythonOperator(task_id="promote_model", python_callable=_promote,
                             inlets=datasets("model"), outlets=datasets("model"))
    keep = EmptyOperator(task_id="keep_champion")
    # S'exécute après l'une OU l'autre branche (l'autre est "skipped", ce n'est pas une erreur)
    score = PythonOperator(task_id="score_batch", python_callable=_score,
                           trigger_rule="none_failed_min_one_success",
                           inlets=datasets("silver", "model"), outlets=datasets("gold"))
    monitor = PythonOperator(task_id="monitor_drift", python_callable=_monitor,
                             inlets=datasets("silver", "model"), outlets=datasets("drift"))
    # Résume les prédictions Gold en petits tableaux lus par le dashboard (http://localhost:8501)
    kpis = PythonOperator(task_id="publish_kpis", python_callable=_kpis,
                          inlets=datasets("gold"), outlets=datasets("kpis"))

    ingest >> validate >> features >> train >> compare >> decide >> [promote, keep] >> score >> monitor >> kpis
