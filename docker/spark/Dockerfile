# Adds the Python deps our PySpark jobs need on top of the base Bitnami Spark image.
FROM bitnami/spark:3.5

USER root
COPY docker/spark/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
USER 1001
