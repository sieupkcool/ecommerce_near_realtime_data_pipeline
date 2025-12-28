from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum
from datetime import timedelta
import os

# Cấu hình kết nối ClickHouse
CLICKHOUSE_CONFIG = {
    'host': 'clickhouse',
    'user': 'airflow',
    'password': 'airflow',
    'database': 'gold'
}

# Đường dẫn file SQL
SQL_FILE_PATH = os.path.join(os.path.dirname(__file__), "etl_job.sql")

def run_etl_job(**context):
    # --- 1. IMPORT THƯ VIỆN ---
    from clickhouse_driver import Client

    vn_tz = pendulum.timezone("Asia/Ho_Chi_Minh")
    
    # --- 2. LẤY THAM SỐ THỜI GIAN ---
    # Lấy thời gian từ Airflow (đã là VN Time do config timezone của DAG hoặc server)
    start_dt = context["data_interval_start"].in_timezone(vn_tz)
    end_dt = context["data_interval_end"].in_timezone(vn_tz)
    
    # --- FIX LOGIC CHẠY TAY (MANUAL TRIGGER) ---
    # Nếu start == end (do chạy tay), ta lùi lại 5 phút để lấy dữ liệu vừa sinh
    if start_dt == end_dt:
        start_dt = end_dt - timedelta(minutes=5)

    # Format thời gian dạng String để bắn vào SQL
    start_ts = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts = end_dt.strftime("%Y-%m-%d %H:%M:%S")

    print(f"=== BẮT ĐẦU ETL JOB ===")
    print(f"Khung giờ xử lý (VN Time): {start_ts} -> {end_ts}")

    # --- 3. ĐỌC FILE SQL TEMPLATE ---
    if not os.path.exists(SQL_FILE_PATH):
        raise FileNotFoundError(f"Không tìm thấy file SQL tại: {SQL_FILE_PATH}")

    with open(SQL_FILE_PATH, 'r') as f:
        sql_template = f.read()
    
    # --- 4. THAY THẾ BIẾN VÀO SQL ---
    # SQL bây giờ đã có hàm toDateTime(..., 'Asia/Ho_Chi_Minh') nên ta cứ truyền string vào
    final_sql = sql_template.format(
        start_ts=start_ts,
        end_ts=end_ts
    )
    
    print(f"Executing SQL:\n{final_sql[:200]} ... (đã cắt ngắn)")

    # --- 5. THỰC THI TRÊN CLICKHOUSE ---
    client = Client(**CLICKHOUSE_CONFIG)
    
    client.execute(final_sql)
    print("SUCCESS: Đã thực thi lệnh INSERT vào bảng Fact thành công.")

    # --- 6. KIỂM TRA NHANH KẾT QUẢ ---
    # Kiểm tra xem ngày hôm nay đã có bao nhiêu dòng trong Fact
    # (Lưu ý: Query này check theo ngày, nên không cần quá chính xác giờ phút)
    check_sql = f"""
        SELECT count() 
        FROM gold.FACT_SALES_PRODUCT 
        WHERE date_key = toYYYYMMDD(toDate('{start_ts}'))
    """
    result = client.execute(check_sql)
    print(f"Kiểm tra nhanh: Bảng Fact hiện có {result[0][0]} dòng cho ngày {start_ts[:10]}.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="gold_fact_sales_product_v3",
    default_args=default_args,
    start_date=pendulum.datetime(2025, 10, 30, tz="UTC"),
    schedule="*/5 * * * *", 
    catchup=False,
    max_active_runs=1,
    tags=["production", "clickhouse"]
) as dag:

    run_etl = PythonOperator(
        task_id="run_insert_fact",
        python_callable=run_etl_job
    )