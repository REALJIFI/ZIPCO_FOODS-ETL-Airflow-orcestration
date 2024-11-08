from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operators import PythonOperator
from Extraction import run_Extraction
from Transformation import run_Transformation
from Load import run_loading

default_args = {
    'owmer': 'airflow',
    'depends on past' : False,
    'start_date' : datetime(2024, 11, 7),
    'email' : 'ifigeorgeifi@yahoo.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries': 1,
    'retries_delay' : timedelta(minutes=1)
}

dag = DAG(
    'zipco_food_pipeline',
    default_args=default_args
    description = 'this represents zipco food management pipeline'
)

Extraction = PythonOperator(
    task_id = 'Extraction_layer',
    python_callable=run_Extraction,
    dag=dag
)

Transformation = PythonOperator(
    task_id = 'Transformation_layer',
    python_callable=run_Transformation,
    dag=dag
)

Load = PythonOperator(
    task_id = 'Load _layer',
    python_callable=run_loading,
    dag=dag
)

Extraction >> Transformation >> Load
    
    
    
    
    