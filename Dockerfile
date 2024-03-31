FROM apache/airflow:2.8.4-python3.11

USER airflow
WORKDIR /opt/airflow
ADD requirements.txt .
RUN pip install apache-airflow==2.8.4 -r requirements.txt