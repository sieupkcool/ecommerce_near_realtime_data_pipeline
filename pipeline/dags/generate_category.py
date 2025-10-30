from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset # <-- THÊM DÒNG NÀY

from ecommerce.models.category import Category

# ĐỊNH NGHĨA MỘT DATASET
CATEGORY_DATASET = Dataset("ecommerce://category")

def generate_category_data(num_cats=1):
    Instance = Category()
    Instance.generate_fake_categories(num_cats)


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ecommerce_generate_category',
    start_date=datetime(2025, 8, 30),
    schedule='@weekly',
    default_args=default_args, catchup=False
) as dag:

    generate_category = PythonOperator(
        task_id='generate_category',
        python_callable=generate_category_data,
        op_args=[20],
        outlets=[CATEGORY_DATASET] # <-- THÊM DÒNG NÀY
    )