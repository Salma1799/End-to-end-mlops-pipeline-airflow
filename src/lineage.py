"""DATA LINEAGE (traçabilité) : OpenLineage + Marquez, et jeux de données visibles dans Airflow.

Chaque tâche du DAG déclare ce qu'elle LIT (inlets) et ce qu'elle ÉCRIT (outlets).
Chaque jeu de données est déclaré sous deux formes :
  1. un Dataset OpenLineage  -> envoyé à Marquez (http://localhost:3000), qui dessine le graphe complet ;
  2. un Dataset Airflow      -> affiché directement dans l'onglet "Graph" d'Airflow
                                (bouton "Show datasets"), entre les tâches qui le produisent.

Les jeux de données sont nommés sans le mois : "zone Silver" est le même jeu de données
pour tous les mois (le mois est une partition year=/month=, pas un nouveau jeu de données).
"""
try:
    from openlineage.client.event_v2 import Dataset as OLDataset
except ImportError:  # librairie absente : le DAG fonctionne quand même, sans Marquez
    OLDataset = None

try:
    from airflow.datasets import Dataset as AirflowDataset
except ImportError:
    AirflowDataset = None

# clé -> (namespace OpenLineage, nom OpenLineage, URI affichée dans Airflow)
_DATASETS = {
    "tlc_open_data": ("https://d37ci6vzurychx.cloudfront.net", "trip-data/yellow_tripdata",
                      "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata"),
    "taxi_zones":    ("taxiflow-datalake", "raw/taxi_zone_lookup",
                      "file:///opt/airflow/data/raw/taxi_zone_lookup.csv"),
    "bronze":        ("taxiflow-datalake", "bronze/yellow",
                      "file:///opt/airflow/data/bronze/yellow"),
    "silver":        ("taxiflow-datalake", "silver/yellow",
                      "file:///opt/airflow/data/silver/yellow"),
    "gold":          ("taxiflow-datalake", "gold/predictions",
                      "file:///opt/airflow/data/gold/predictions"),
    "model":         ("mlflow", "registry/taxiflow-duration-model",
                      "mlflow://mlflow:5000/models/taxiflow-duration-model"),
    "quality":       ("taxiflow-reports", "quality_report",
                      "file:///opt/airflow/reports/quality"),
    "drift":         ("taxiflow-reports", "drift_report",
                      "file:///opt/airflow/reports/drift"),
    "kpis":          ("taxiflow-reports", "kpis_dashboard",
                      "file:///opt/airflow/reports/kpis"),
}


def datasets(*keys: str) -> list:
    """Renvoie les jeux de données demandés, ex. datasets("bronze", "taxi_zones").

    Pour chaque clé : la version OpenLineage (pour Marquez) + la version Airflow (pour l'onglet Graph).
    OpenLineage ignore les Datasets Airflow, et Airflow n'affiche que les siens : pas de doublon.
    """
    result = []
    for key in keys:
        namespace, name, uri = _DATASETS[key]
        if OLDataset is not None:
            result.append(OLDataset(namespace=namespace, name=name))
        if AirflowDataset is not None:
            result.append(AirflowDataset(uri))
    return result
