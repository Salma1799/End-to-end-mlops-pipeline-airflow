# Image Airflow + Java (pour Spark) + librairies du projet
FROM apache/airflow:2.11.2-python3.11

USER root
# Java est indispensable pour faire tourner Spark ; procps fournit la commande "ps" utilisée par Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /requirements.txt
# On fixe la version d'Airflow dans la même commande pour qu'elle ne soit jamais modifiée
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# Traçabilité (Data Lineage) : provider OpenLineage, installé dans une couche séparée
# -> l'ajouter ne force pas Docker à retélécharger PySpark et les autres librairies
# cryptography < 46 : version compatible avec pyOpenSSL (sinon l'interface web d'Airflow plante)
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" "apache-airflow-providers-openlineage==2.11.0" "cryptography==44.0.3"
