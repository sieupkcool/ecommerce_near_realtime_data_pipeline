from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from ecommerce.models.province import Province
from ecommerce.models.city import City


def generate_provinces():
    Instance = Province()
    Instance.generate_us_states()


def generate_cities():
    Instance = City()
    Instance.generate_cities_for_provinces()


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('ecommerce_generate_province_and_city',
         start_date=datetime(2025, 9, 30),
         schedule="@once",
         default_args=default_args, catchup=False) as dag:

    generate_provinces_info = PythonOperator(
        task_id='generate_provinces_info',
        python_callable=generate_provinces
    )

    generate_cities_info = PythonOperator(
        task_id='generate_cities_info',
        python_callable=generate_cities
    )

generate_provinces_info >> generate_cities_info
