from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'khanhxoe',
    'start_date': datetime(2026, 4, 19, 10, 20),
}

def get_api_date():
    import requests
    return requests.get("https://randomuser.me/api/").json()['results'][0]

def format_data(response):
    data = {}
    data['name'] = response['name']['first'] + ' ' + response['name']['last']
    data['age'] = response['dob']['age']
    data['gender'] = response['gender']
    data['email'] = response['email']
    data['phone'] = response['phone']
    data['cell'] = response['cell']
    data['id'] = response['id']['value']
    data['picture'] = response['picture']['large']
    data['nat'] = response['nat']
    data['registered_date'] = response['registered']['date']

    return data


def kafka_stream():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    logging.info('Starting kafka stream')

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        max_block_ms=5000
    )
    curr = time.time()

    while True:
        if (time.time() - curr) > 60:
            break
        try:
            response = get_api_date()
            formated_data = format_data(response)
            producer.send('user_created', value=json.dumps(formated_data).encode('utf-8'))
            producer.flush()
            time.sleep(1)
            logging.info('Sent data to Kafka topic: user_created')

        except Exception as e:
            logging.error(f'Error: {e}')
            continue
            
    logging.info('Kafka stream ended')

with DAG(
    'kafka_stream',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='kafka_stream_task',
        python_callable=kafka_stream
    )