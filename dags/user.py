import json
from re import I
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator 
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from pandas import json_normalize

from datetime import datetime

from requests import head

with DAG('user', start_date=datetime(2022, 7, 11), schedule_interval="@daily", catchup=False) as dag:
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="postgres",
        sql=''' 
        CREATE TABLE IF NOT EXISTS users(
            firstname text not null,
            lastname text not null,
            country text not null,
            username text not null,
            password text not null,
            email text not null,
            )
            '''
    ) 
    
    is_api_aviable = HttpSensor(
        task_id='is_api_aviable',
        http_conn_id='user_api',
        endpoint='api/',
    )
    
    users = SimpleHttpOperator(
        task_id='extract_user',
        http_conn_id='user_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )
    
    def _process_users_(ti):
        user = ti.xcom_pull(task_ids='extrac_user')
        user = user['results'][0]
        processed_user = json_normalize({
             'firstname': user['name']['firts'],
            'lastname': user['name']['last'],
            'country': user['location']['country'],
            'username': user['login']['username'],
            'password': user['login']['password'],
            'email': user['email']
        })
        processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)
    
    process_users = PythonOperator(
        task_id='process_users',
        python_callable=_process_users_
        
    )