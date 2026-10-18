# Serveur MLflow (tracking + Model Registry)
FROM python:3.11-slim

RUN pip install --no-cache-dir mlflow==3.17.0

ENV MLFLOW_DISABLE_TELEMETRY=true \
    DO_NOT_TRACK=true

RUN mkdir -p /mlflow/artifacts
WORKDIR /mlflow
EXPOSE 5000
