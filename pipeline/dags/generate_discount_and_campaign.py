from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def generate_ads_campaign_data(num_campaigns=1, execution_date_str=None):
    from ecommerce.models.ads_campaign import AdsCampaigns
    Instance = AdsCampaigns()
    Instance.generate_ad_campaigns(num_campaigns=num_campaigns, execution_date_str=execution_date_str)

def generate_discount_data(num_discount=1, execution_date_str=None):
    from ecommerce.models.discount import Discount
    Instance = Discount()
    Instance.generate_discounts(num_discounts=num_discount, execution_date_str=execution_date_str)


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'ecommerce_generate_discount_and_campaign',
    start_date=datetime(2025, 10, 30),
    schedule='@weekly',
    default_args=default_args, 
    catchup=True,
    max_active_runs=1
) as dag:

    generate_campaign = PythonOperator(
        task_id='generate_campaign',
        python_callable=generate_ads_campaign_data,
        op_args=[5, '{{ ds }}']
    )

    generate_discount = PythonOperator(
        task_id='generate_discount',
        python_callable=generate_discount_data,
        op_args=[5, '{{ ds }}']
    )

generate_campaign >> generate_discount
