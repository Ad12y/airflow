from airflow.decorators import dag, task 
from datetime import datetime
import random


@dag(
    start_date = datetime(2024,1,1), 
    schedule = "@0daily",
    cathup = True, 
    tags = ["Task 1"]
)

def task_1():

    @task
    def rand_numb():
        return random.randit()
    
    @task
    def even_odd(value):
        if not value%2:
            return "Even"
        return "odd"
    
    even_odd(rand_numb())

task_1()