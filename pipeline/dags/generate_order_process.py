from datetime import datetime, timedelta

from airflow import AirflowException

from ecommerce.models.order import OrderRegistration
from ecommerce.models.transaction import Transaction

from airflow.decorators import dag, task

from airflow.sensors.external_task import ExternalTaskSensor

@dag(
    dag_id='ecommerce_generate_order_process',
    start_date=datetime(2025, 9, 30),
    schedule=timedelta(minutes=1),
    catchup=False,
)
def order_process():

    wait_for_user_registration = ExternalTaskSensor(
        task_id='wait_for_user_registration',
        # Tên DAG mà nó cần đợi
        external_dag_id='ecommerce_user_registration', 
        # Tên task trong DAG đó mà nó cần đợi
        external_task_id='generate_user_address',
        mode='poke',
        poke_interval=15,
        timeout=300,
        # Đảm bảo nó đợi lần chạy gần nhất
        execution_delta=timedelta(minutes=1) 
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

    wait_for_user_registration >> generate_transaction_for_order(generate_order_process())


order_process()
