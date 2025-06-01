from datetime import datetime, timedelta
import time
from kafka import KafkaProducer
import requests
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# A DAG represents a workflow, a collection of tasks
# with DAG(dag_id="first_project", start_date=datetime(2024, 1, 1), schedule="0 0 * * *") as dag:
#     # Tasks are represented as operators
#     hello = BashOperator(task_id="hello", bash_command="echo hello")

#     @task()
#     def test():
#         print('hello')


#     # Set dependencies between tasks
#     hello >> airflow()



# Default arguments for the DAG
default_args = {
    'owner': 'Youssef',
    'depends_on_past': False,
    'email': ['youssef@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configs
api_url = "https://randomuser.me/api/"
kafka_broker = 'kafka:9092'
kafka_topic = 'users'


# Function to fetch data from api
def get_user_data_api():
    data = requests.get(api_url)
    data = data.json()['results']
    # print(json.dumps(res,indent=3))
    return data

# Function to be used in PythonOperator
def stream_to_kafka():
    time_break = time.time()
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        if time.time() > time_break + 30 : break
        data = get_user_data_api()
        # print(data)
        # print("####################")
        producer.send(kafka_topic, value=data)
                
        

# Define the DAG
dag = DAG(
    'first_project_demo',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example']
)

# Define the tasks
start_task = BashOperator(
    task_id='start',
    bash_command='echo "Start of the DAG"',
    dag=dag,
)

python_task = PythonOperator(
    task_id='stream_to_kafka',
    python_callable=stream_to_kafka,
    dag=dag,
)

end_task = BashOperator(
    task_id='end',
    bash_command='echo "End of the DAG"',
    dag=dag,
)

# Define task dependencies
start_task >> python_task >> end_task

# stream_to_kafka()