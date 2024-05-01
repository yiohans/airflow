from airflow.decorators import dag, task
from datetime import datetime

@task
def add_task(x, y):
    print(f"Task args: x={x}, y={y}")
    return x + y

def create_dynamic_dag(dag_id, schedule, start_date, description="", catchup=False, is_paused_upon_creation=False, tags=[]):
    @dag(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=start_date,
        catchup=catchup,
        is_paused_upon_creation=is_paused_upon_creation,
        tags=tags
    )
    def example_dag():
        
        start = add_task.override(priority_weight=3)(1, 2)
        for i in range(3):
            start >> add_task.override(task_id=f"new_add_task_{i}", retries=4)(start, i)

    return example_dag()

for i in range(5):
    globals()[f"dag{i+1}"] = create_dynamic_dag(dag_id=f"dynamic_dag{i+1}", schedule='@hourly', start_date=datetime(2023,1,1), is_paused_upon_creation=False)
