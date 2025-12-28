from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
from clickhouse_driver import Client

# Cấu hình kết nối ClickHouse
CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'user': 'airflow',
    'password': 'airflow',
    'database': 'silver'
}

def check_data_job(**context):
    # --- 1. XỬ LÝ THỜI GIAN ---
    vn_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
    
    start_dt = context["data_interval_start"].in_timezone(vn_tz)
    end_dt = context["data_interval_end"].in_timezone(vn_tz)
    
    is_manual = False
    if start_dt == end_dt:
        is_manual = True
        start_dt = end_dt - timedelta(minutes=5)

    # Format chuẩn
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"\n{'='*30}")
    print(f"🛑 DEBUG MODE: {( 'MANUAL RUN' if is_manual else 'SCHEDULED RUN' )}")
    print(f"⏰ Khung giờ check (VN): {start_ts}  -->  {end_ts}")
    print(f"{'='*30}\n")

    client = Client(**CLICKHOUSE_CONFIG)

    # --- 2. QUERY 1: ĐẾM TỔNG SỐ ĐƠN (SỬA LẠI SQL) ---
    # THAY ĐỔI QUAN TRỌNG: Thêm toDateTime(..., 'Asia/Ho_Chi_Minh')
    sql_total = f"""
        SELECT count(), uniq(order_status_id)
        FROM silver.orders
        WHERE created_at >= toDateTime('{start_ts}', 'Asia/Ho_Chi_Minh') 
          AND created_at <  toDateTime('{end_ts}', 'Asia/Ho_Chi_Minh')
    """
    
    # In ra để bạn yên tâm là nó giống hệt DBeaver
    print(f"Executing SQL Total:\n{sql_total}")
    
    res_total = client.execute(sql_total)
    total_count = res_total[0][0]
    
    # --- 3. QUERY 2: ĐẾM ĐƠN HỢP LỆ (SỬA LẠI SQL) ---
    sql_valid = f"""
        SELECT count()
        FROM silver.orders
        WHERE created_at >= toDateTime('{start_ts}', 'Asia/Ho_Chi_Minh') 
          AND created_at <  toDateTime('{end_ts}', 'Asia/Ho_Chi_Minh')
          AND order_status_id = 4
    """
    res_valid = client.execute(sql_valid)
    valid_count = res_valid[0][0]

    # --- 4. IN KẾT QUẢ ---
    print(f"📊 KẾT QUẢ KIỂM TRA:")
    print(f"   + Tổng đơn trong khung giờ: {total_count}")
    print(f"   + Số đơn 'Delivered' (id=4): {valid_count}")
    
    if total_count == 0:
        print("⚠️ CẢNH BÁO: Không tìm thấy đơn nào.")
    elif valid_count == 0:
        print("⚠️ CẢNH BÁO: Có đơn hàng nhưng không có đơn nào là 'Delivered'.")
    else:
        print("✅ DỮ LIỆU TỐT: Khớp với DBeaver!")

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="debug_check_silver_data",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
    schedule="*/5 * * * *", 
    catchup=False,
    tags=["debug", "test"]
) as dag:

    run_check = PythonOperator(
        task_id="run_check_query",
        python_callable=check_data_job
    )