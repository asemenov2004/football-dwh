FROM apache/airflow:2.9.3-python3.11

USER airflow

COPY requirements.txt /tmp/airflow-requirements.txt
RUN pip install --no-cache-dir -r /tmp/airflow-requirements.txt
