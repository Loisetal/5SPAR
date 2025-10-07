FROM jupyter/pyspark-notebook:latest

USER root

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt || true

RUN pip install --no-cache-dir Mastodon.py confluent-kafka python-dotenv

USER jovyan
