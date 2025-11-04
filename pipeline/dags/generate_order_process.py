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
        order_ids_list = order.generate_bulk_orders(1000) # <-- TẠO 1000 ĐƠN
        
        print(f"Đã tạo {len(order_ids_list)} ID đơn hàng.")
        
        # Kiểm tra xem list có rỗng không
        if not order_ids_list:
            raise AirflowException('Không tạo được đơn hàng nào...')
            
        # Trả về DANH SÁCH ID
        return order_ids_list

    @task(retries=5, retry_delay=timedelta(minutes=5))
    def generate_transaction_for_order(order_ids):
        transaction = Transaction()
        transaction.generate_bulk_transactions(order_ids)

    order_ids_result = generate_order_process()
    wait_for_customer >> order_ids_result
    generate_transaction_for_order(order_ids_result)


order_process()
