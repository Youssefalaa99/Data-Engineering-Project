from datetime import datetime, timedelta
import time,json
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


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
kafka_broker = 'broker:29092'
kafka_topic = 'users'
extract_duration = 30    # The duration to keep extracting from API in (seconds)


# Function to fetch data from api
def get_user_data_api():
    import requests
    
    data = requests.get(api_url)
    data = data.json()['results'][0]
    # print(json.dumps(data,indent=3))
    return data

def format_data(data):
    res = {}
    location = data['location']

    res['first_name'] = data['name']['first']
    res['last_name'] = data['name']['last']
    res['gender'] = data['gender']
    res['address'] = f"{location['street']['number']}, {location['street']['name']} | {location['city']}, {location['state']} - {location['country']}"
    res['postcode'] = location['postcode']
    res['email'] = data['email']
    res['username'] = data['login']['username']
    res['age'] = data['dob']['age']
    res['registered_date'] = data['registered']['date']
    res['phone'] = data['phone']

    return res


# Function to be used in PythonOperator
def stream_to_kafka():
    import logging 

    time_break = time.time()
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker, 
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    while True:
        if time.time() > time_break + extract_duration : break
        try:
            data = get_user_data_api()
            data = format_data(data)
            producer.send(kafka_topic, value=data)
        except Exception as e:
            logging.error(f"Error while producing to kafka {e}")
                
        

# # Define the DAG
with DAG(
    dag_id='stream_to_kafka',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example']  
) as dag:

    # Define the tasks
    start_task = BashOperator(
        task_id='start',
        bash_command='echo "Start of the DAG"',
    )

    python_task = PythonOperator(
        task_id='stream_to_kafka',
        python_callable=stream_to_kafka,
    )

    end_task = BashOperator(
        task_id='end',
        bash_command='echo "End of the DAG"',
    )

    # Define task dependencies
    start_task >> python_task >> end_task


# Test individual functions
# stream_to_kafka()
# get_user_data_api()