import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
import pandas as pd

class Region:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.csv_path = '/opt/airflow/data/vncities.csv'

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def generate_vn_regions(self):
        """
        Đọc file CSV, lấy các bang duy nhất và chèn vào bảng 'provinces'.
        """
        try:
            df_csv = pd.read_csv(self.csv_path)
            
            unique_regions = df_csv['region_name'].unique()
            
            region_data = [(name,) for name in unique_regions]
            
            if region_data:
                query = "INSERT INTO regions (region_name) VALUES %s ON CONFLICT (region_name) DO NOTHING"
                execute_values(self.cur, query, region_data)
                self.conn.commit()
                print(f"Đã chèn thành công {len(region_data)} miền.")
            else:
                print("Không tìm thấy miền nào trong file CSV.")

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo miền từ CSV: {e}")
            raise

def main():
    province_model_generator = Region()
    province_model_generator.generate_vn_regions()

if __name__ == "__main__":
    main()