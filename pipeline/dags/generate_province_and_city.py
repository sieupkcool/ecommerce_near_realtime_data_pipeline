from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_regions():
    from ecommerce.models.region import Region
    Instance = Region()
    Instance.generate_vn_regions()


def generate_provinces():
    from ecommerce.models.province import Province
    Instance = Province()
    Instance.generate_provinces_for_regions()


default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG('ecommerce_generate_region_and_province',
         start_date=datetime(2025, 10, 30),
         schedule="@once",
         default_args=default_args, 
         catchup=True,
        max_active_runs=1
) as dag:

    generate_regions_info = PythonOperator(
        task_id='generate_regions_info',
        python_callable=generate_regions
    )

    generate_provinces_info = PythonOperator(
        task_id='generate_provinces_info',
        python_callable=generate_provinces
    )

generate_regions_info >> generate_provinces_info
