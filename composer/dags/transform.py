from airflow.decorators import dag, task
from datetime import datetime, timedelta


@dag(
    start_date=datetime(2026,1,1),
    schedule=None,
    catchup=False,
    default_args={
         'retries': 2, # will retry this dag upto 2 times before failing it. 
         'retry_delay': timedelta(seconds=2) # how much to wait between retries
        #  'retry_exponential_backoff': True, # after each time a task fails, it will wait exponentially, each time it will double
        #  'max_try_delay': timedelta(hours=1) # the maximum time the task will wait until failure
         
     },
    tags=['transform', 'sales-warehouse']
)
def transform():
    @task
    def first_task():
        print("it works!")
    
    first_task()
transform()