from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException

# Định nghĩa DAG
@dag(
    dag_id='ecommerce_test_single_order_manual',  
    start_date=datetime(2023, 1, 1),
    schedule=None, 
    catchup=False,
    tags=['test', 'demo', 'manual'], 
)
def test_single_order():

    @task(task_id="generate_one_order")
    def generate_single_order():
        from ecommerce.models.order import OrderRegistration
        
        print("--- [START] Bắt đầu sinh 1 đơn hàng test ---")
        order_model = OrderRegistration()
        
        order_ids = order_model.generate_delivered_orders(num_orders=1)
        
        if not order_ids:
            raise AirflowException("Lỗi: Không sinh được đơn hàng.")
            
        print(f"Đã sinh Order ID: {order_ids[0]}")
        return order_ids

    @task(task_id="generate_transaction")
    def create_transaction_and_update_status(order_ids: list):
        from ecommerce.models.transaction import Transaction
        
        if not order_ids:
            print("Không có order ID nào để xử lý.")
            return

        print(f"Đang tạo transaction cho Order ID: {order_ids[0]}...")
        trans_model = Transaction()
        trans_model.generate_bulk_transactions(order_ids)
        print("Đã tạo transaction và cập nhật trạng thái.")

    created_orders = generate_single_order()
    create_transaction_and_update_status(created_orders)

test_single_order()