FROM apache/airflow:2.10.5-python3.11

USER airflow

COPY airflow/requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
