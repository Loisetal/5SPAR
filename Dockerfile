FROM jupyter/pyspark-notebook:latest

USER root

# Installer les dépendances Python nécessaires
RUN pip install --no-cache-dir \
    mastodon.py \
    kafka-python \
    psycopg2-binary \
    matplotlib \
    seaborn \
    pandas

USER jovyan