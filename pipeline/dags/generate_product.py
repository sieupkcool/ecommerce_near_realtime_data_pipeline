from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset 

from ecommerce.models.product import Product
from ecommerce.models.product_tag import ProductTag

# IMPORT 2 DATASET TỪ CÁC FILE TRÊN
BRAND_DATASET = Dataset("ecommerce://brand")
CATEGORY_DATASET = Dataset("ecommerce://category")


def generate_product_data(num_product):
    product = Product()
    product.generate_fake_products(num_product)


def generate_tag_product(num_tag):
    product_tag = ProductTag()
    product_tag.generate_product_tag_associations(num_tag)


default_args = {
    'owner': 'airflow',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

with DAG('ecommerce_generate_product',
        start_date=datetime(2025, 8, 30),
        # THAY THẾ LỊCH CHẠY BẰNG CÁC DATASET
        schedule=[BRAND_DATASET, CATEGORY_DATASET],
         default_args=default_args, catchup=False) as dag:

    generate_product = PythonOperator(
        task_id='generate_product',
        python_callable=generate_product_data,
        op_args=[25],
    )

    generate_tag_for_product = PythonOperator(
        task_id='generate_tag_for_product',
        python_callable=generate_tag_product,
        op_args=[100],
    )

generate_product >> generate_tag_for_product