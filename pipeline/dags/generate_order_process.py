from datetime import datetime, timedelta

from airflow import AirflowException

from ecommerce.models.order import OrderRegistration
from ecommerce.models.transaction import Transaction
from ecommerce.models.user import User

from airflow.decorators import dag, task

from airflow.sensors.python import PythonSensor

def check_customer_user_exists():
    user_instance = User()
    exists = user_instance.has_customer_user()
    if exists:
        print("Found customer user. Proceeding...")
        return True
    else:
        print("Waiting for a 'customer' user to be created...")
        return False

@dag(
    dag_id='ecommerce_generate_order_process',
    start_date=datetime(2025, 9, 30),
    schedule=timedelta(minutes=1),
    catchup=False,
)
def order_process():

    wait_for_customer = PythonSensor(
        task_id="wait_for_customer_user",
        python_callable=check_customer_user_exists,
        poke_interval=15,
        timeout=600,
        mode="poke"
    )

    @task(retries=5, retry_delay=timedelta(minutes=5))
    def generate_order_process():
        order = OrderRegistration()
        order_id = order.save_order()
        print(order_id)
        if order_id:
            return order_id

        raise AirflowException('Order not found...')

    @task(retries=5, retry_delay=timedelta(minutes=5))
    def generate_transaction_for_order(order_id):
        transaction = Transaction()
        transaction.generate_transaction(order_id)

    order_id = generate_order_process()
    wait_for_customer >> order_id
    generate_transaction_for_order(order_id)


order_process()
