from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Define the DAG
@dag(
    schedule=[Dataset("dataset1a"),Dataset("dataset1b"),Dataset("dataset1c")],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["datasets"]
)

def consumer1():
    # Define the tasks using decorators
    @task
    def consumedatasets():
        print("Task 1 executed")
    consumedatasets()

consumer1()