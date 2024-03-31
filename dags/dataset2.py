from airflow import Dataset
from airflow.decorators import dag, task
from datetime import datetime

# Define the DAG
@dag(schedule='*/2 * * * *', start_date=datetime(2024, 1, 1), catchup=False)
def update_dataset2():
    # Define the tasks using decorators
    @task(outlets=[Dataset("dataset2a")])
    def dataset2a():
        print("Task 1 executed")
        #wait some time
        import time
        time.sleep(40)
    dataset2a()

    @task(outlets=[Dataset("dataset2b")])
    def dataset2b():
        print("Task 2 executed")
        #wait some time
        import time
        time.sleep(60)
    dataset2b()
update_dataset2()