"""Création d'une session Spark légère (mode local), adaptée à un PC de 8 Go."""
import os
import sys

from pyspark.sql import SparkSession


def get_spark(app_name: str = "taxiflow") -> SparkSession:
    # Même interpréteur Python pour le driver et les workers Spark
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    return (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER", "local[2]"))
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "1g"))
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
