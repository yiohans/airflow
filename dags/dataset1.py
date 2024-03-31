from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Define the DAG
@dag(schedule='*/2 * * * *', start_date=datetime(2024, 1, 1), catchup=False)
def update_dataset1():
    # Define the tasks using decorators
    @task(outlets=[Dataset("dataset1a")])
    def dataset1a():
        print("Task 1 executed")
    dataset1a()

    @task(outlets=[Dataset("dataset1b")])
    def dataset1b():
        print("Task 2 executed")
        #wait some time
        import time
        time.sleep(40)
    dataset1b()

    @task(outlets=[Dataset("dataset1c")])
    def dataset1c():
        print("Task 3 executed")
        #wait some time
        import time
        time.sleep(15)
    dataset1c()
update_dataset1()