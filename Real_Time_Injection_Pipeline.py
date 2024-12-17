from airflow import DAG
from datetime import datetime

from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.https.sensors.http import HttpSennsor
from airflow.providers.httpsoperators.http import HttpSennsor
from airflow.operators.python import PythonOperators
from airflow.provides.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize

def def_process_user(ti):
   user=ti.xcom_pull(task_ids="extract_user")
   user = user['results'][0]
   processed_user=json_normalize({
    'firstname': user['name']['first'],
    'lastname': user['name']['last'],
    'country': user['name']['country'],
    'username': user['name']['username'],
    'password': user['name']['password'],
    'email': user['name']['email'],
      
   })
   processed_user.to_csv('/tmp/processed_user.csv', index=None,header=False)


def def_store_user():
    hook = PostgresHook(postgres_conn_id='postgres')
    hook.copy_expert(
        sql="COPY users from stdin WITH DELIMITER as ',",
        filename='/tmp/processed_user.csv'
    )


with DAG(
    dag_id = "Real_Time_Injection_Pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    Catchup=False
) as dag:

   create_table = PostgresOperator(
      task_id = 'create_table',
      postgres_conn_id='postgres',
      sql='''
         CREATE TABLE IF NOT EXISTS users(
         firstname TEXT NOT NULL,
         lastname TEXT NOT NULL,
         country TEXT NOT NULL,
         username TEXT NOT NULL,
         password TEXT NOT NULL,
         email TEXT NOT NULL,
         );
      ''')
   api_Available = HttpSennsor(
        task_id='api_Available',
        http_conn_id='api_connect',
        end_point='api/'
    )
extract_user= SimpletterHttpOperator(
   task_id='extract_user',
   http_connect_id='user_api',
   endpoint='api/',
   method='GET',
   respose_filter=lambda response: json.loads(response.text),
   log_response=True
)
process_user = PythonOperators(
   tasl_id='process_user',
   python_callable=def_process_user
)
target_store_user = PythonOperators
    task_id = 'target_store_user',
    python_collable=def_store_user


