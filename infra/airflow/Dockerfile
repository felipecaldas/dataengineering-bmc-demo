FROM apache/airflow:3.3.0-python3.12

COPY infra/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==3.3.0" -r /tmp/requirements.txt

