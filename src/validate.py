"""Étape 2 du pipeline : CONTRÔLE QUALITÉ avec Great Expectations.

Vérifie les données d'un mois en zone Bronze avec des règles métier
(durée, distance, prix, zones, nombre de passagers...).

Principe : on tolère un petit pourcentage de lignes aberrantes (elles seront filtrées
à l'étape suivante), mais si une règle est trop souvent violée, ou si la structure
des données est cassée, le pipeline S'ARRÊTE : on n'entraîne jamais un modèle sur des données douteuses.
"""
import json
import os
from datetime import datetime

import great_expectations as gx
from pyspark.sql import functions as F

from ingest import bronze_path
from spark_utils import get_spark

REPORTS_DIR = os.getenv("REPORTS_DIR", "/opt/airflow/reports")
MIN_ROWS = int(os.getenv("QUALITY_MIN_ROWS", "100000"))

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
    "passenger_count", "PULocationID", "DOLocationID", "fare_amount",
]


class DataQualityError(Exception):
    """Levée quand les données ne respectent pas les règles de qualité : le DAG s'arrête."""


def month_bounds(month: str):
    start = datetime.strptime(month, "%Y-%m")
    end = start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)
    return start, end


def build_rules(month: str) -> list:
    """Les règles de qualité. 'mostly' = part minimale de lignes qui doivent respecter la règle."""
    start, end = month_bounds(month)
    E = gx.expectations
    return [
        # --- Règles bloquantes : la structure des données doit être correcte ---
        ("Le lot contient assez de courses", E.ExpectTableRowCountToBeBetween(min_value=MIN_ROWS)),
        *[(f"La colonne {c} existe", E.ExpectColumnToExist(column=c)) for c in REQUIRED_COLUMNS],
        ("Heure de départ renseignée", E.ExpectColumnValuesToNotBeNull(column="tpep_pickup_datetime", mostly=0.99)),
        ("Zone de départ renseignée", E.ExpectColumnValuesToNotBeNull(column="PULocationID", mostly=0.99)),
        ("Zone d'arrivée renseignée", E.ExpectColumnValuesToNotBeNull(column="DOLocationID", mostly=0.99)),
        # --- Règles métier : on tolère au plus 5 % de lignes aberrantes ---
        ("Départ dans le mois traité", E.ExpectColumnValuesToBeBetween(
            column="tpep_pickup_datetime", min_value=start, max_value=end, strict_max=True, mostly=0.99)),
        ("Durée entre 1 min et 3 h", E.ExpectColumnValuesToBeBetween(
            column="duration_min", min_value=1, max_value=180, mostly=0.95)),
        ("Distance entre 0,1 et 100 miles", E.ExpectColumnValuesToBeBetween(
            column="trip_distance", min_value=0.1, max_value=100, mostly=0.95)),
        # Tolérance plus large : dans les données NYC, 5 à 10 % des courses n'ont pas de nombre de passagers
        ("Entre 1 et 6 passagers", E.ExpectColumnValuesToBeBetween(
            column="passenger_count", min_value=1, max_value=6, mostly=0.85)),
        ("Prix positif (0 à 1000 $)", E.ExpectColumnValuesToBeBetween(
            column="fare_amount", min_value=0, max_value=1000, mostly=0.95)),
        ("Zone de départ valide (1 à 265)", E.ExpectColumnValuesToBeBetween(
            column="PULocationID", min_value=1, max_value=265, mostly=0.99)),
        ("Zone d'arrivée valide (1 à 265)", E.ExpectColumnValuesToBeBetween(
            column="DOLocationID", min_value=1, max_value=265, mostly=0.99)),
    ]


def add_duration(df):
    """Ajoute la durée de la course en minutes (notre future cible de prédiction)."""
    return df.withColumn(
        "duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60.0,
    )


def write_html_report(summary: dict, path: str) -> None:
    """Petit rapport HTML lisible, à ouvrir dans le navigateur pendant la démo."""
    rows = "".join(
        f"<tr class='{'ok' if r['success'] else 'ko'}'><td>{'✅' if r['success'] else '❌'}</td>"
        f"<td>{r['rule']}</td><td>{r['unexpected_percent']}</td><td>{r['tolerance']}</td></tr>"
        for r in summary["rules"]
    )
    status = "VALIDÉ ✅" if summary["success"] else "BLOQUÉ ❌"
    html = f"""<!doctype html><html lang="fr"><head><meta charset="utf-8">
<title>Qualité {summary['month']}</title>
<style>body{{font-family:Segoe UI,Arial,sans-serif;margin:32px;color:#222}}
table{{border-collapse:collapse;width:100%;max-width:900px}}td,th{{border:1px solid #ddd;padding:8px;text-align:left}}
th{{background:#f3f3f3}}.ok td{{background:#f4fbf4}}.ko td{{background:#fdeeee}}
.kpi{{display:inline-block;margin:0 24px 16px 0;padding:12px 16px;border:1px solid #ddd;border-radius:8px}}</style></head>
<body><h1>Contrôle qualité — {summary['month']} : {status}</h1>
<div class="kpi">Courses vérifiées<br><b>{summary['rows_checked']:,}</b></div>
<div class="kpi">Courses valides<br><b>{summary['rows_valid']:,} ({summary['valid_percent']} %)</b></div>
<div class="kpi">Règles respectées<br><b>{summary['rules_passed']} / {summary['rules_total']}</b></div>
<table><tr><th></th><th>Règle</th><th>% de lignes en erreur</th><th>Tolérance max</th></tr>{rows}</table>
</body></html>"""
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)


def validate_month(month: str) -> dict:
    spark = get_spark(f"validate-{month}")
    df = add_duration(spark.read.parquet(bronze_path(month))).cache()
    rows_checked = df.count()

    # 1. Validation Great Expectations
    context = gx.get_context(mode="ephemeral")
    batch_def = (
        context.data_sources.add_spark(name="bronze")
        .add_dataframe_asset(name=f"yellow_{month}")
        .add_batch_definition_whole_dataframe("batch")
    )
    rules = build_rules(month)
    suite = context.suites.add(gx.ExpectationSuite(name=f"taxiflow_quality_{month}"))
    for _, expectation in rules:
        suite.add_expectation(expectation)
    validation = context.validation_definitions.add(
        gx.ValidationDefinition(name=f"validation_{month}", data=batch_def, suite=suite)
    )
    result = validation.run(batch_parameters={"dataframe": df})

    # Great Expectations ne garde pas l'ordre des règles : on associe chaque résultat à sa règle par son identifiant
    results_by_id = {r.expectation_config.id: r for r in result.results}
    rule_results = []
    for label, expectation in rules:
        r = results_by_id[expectation.id]
        pct = r.result.get("unexpected_percent")
        mostly = getattr(expectation, "mostly", None)
        rule_results.append({
            "rule": label,
            "success": bool(r.success),
            "unexpected_percent": "-" if pct is None else f"{pct:.2f} %",
            "tolerance": "-" if mostly in (None, 1.0) else f"{(1 - mostly) * 100:.0f} %",
        })

    # 2. Part des courses qui respectent TOUTES les règles métier (elles iront en zone Silver)
    start, end = month_bounds(month)
    valid = df.filter(
        (F.col("tpep_pickup_datetime") >= F.lit(start)) & (F.col("tpep_pickup_datetime") < F.lit(end))
        & F.col("duration_min").between(1, 180)
        & F.col("trip_distance").between(0.1, 100)
        & F.col("passenger_count").between(1, 6)
        & F.col("fare_amount").between(0, 1000)
        & F.col("PULocationID").between(1, 265)
        & F.col("DOLocationID").between(1, 265)
    ).count()
    df.unpersist()

    summary = {
        "month": month,
        "success": bool(result.success),
        "rows_checked": rows_checked,
        "rows_valid": valid,
        "valid_percent": round(100 * valid / rows_checked, 2) if rows_checked else 0.0,
        "rules_total": len(rule_results),
        "rules_passed": sum(r["success"] for r in rule_results),
        "rules": rule_results,
    }

    os.makedirs(REPORTS_DIR, exist_ok=True)
    with open(f"{REPORTS_DIR}/quality_{month}.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    write_html_report(summary, f"{REPORTS_DIR}/quality_{month}.html")

    for r in rule_results:
        print(f"[Qualité] {'OK ' if r['success'] else 'KO '} {r['rule']} (erreurs : {r['unexpected_percent']})")
    print(f"[Qualité] {month} : {summary['rules_passed']}/{summary['rules_total']} règles OK, "
          f"{valid:,}/{rows_checked:,} courses valides ({summary['valid_percent']} %)")

    if not result.success:
        failed = [r["rule"] for r in rule_results if not r["success"]]
        raise DataQualityError(f"Qualité insuffisante pour {month}. Règles non respectées : {failed}")
    return summary


if __name__ == "__main__":
    import sys
    validate_month(sys.argv[1] if len(sys.argv) > 1 else "2024-01")
