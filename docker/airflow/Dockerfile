# Airflow's official images pin Python 3.12 (no 3.13 support yet), independent
# of the 3.13 used by the rest of the platform's pipeline code — see docker/README.md.
FROM apache/airflow:2.10.3-python3.12

COPY docker/airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
