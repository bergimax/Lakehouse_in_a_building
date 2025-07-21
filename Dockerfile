FROM apache/airflow:2.8.3-python3.10

# Provider e librerie extra
RUN pip install \
      apache-airflow-providers-databricks \
      apache-airflow-providers-amazon \
      apache-airflow-providers-kafka \
      rdflib==7.0.0

# Copia DAG e script
COPY dags/ /opt/airflow/dags/
COPY scripts/ /usr/local/bin/
