from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset # <-- THÊM DÒNG NÀY

from ecommerce.models.brand import Brand

# ĐỊNH NGHĨA MỘT DATASET
BRAND_DATASET = Dataset("ecommerce://brand")

def generate_brand_data(num_brands=1):
    Instance = Brand()
    Instance.generate_fake_brands(num_brands)


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ecommerce_generate_brand',
    start_date=datetime(2025, 8, 30),
    schedule='@weekly',
    default_args=default_args, catchup=False
) as dag:

    generate_brand = PythonOperator(
        task_id='generate_brand',
        python_callable=generate_brand_data,
        op_args=[5],
        outlets=[BRAND_DATASET] # <-- THÊM DÒNG NÀY
    )