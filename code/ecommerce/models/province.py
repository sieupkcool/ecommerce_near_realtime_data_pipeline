import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
import pandas as pd

class Province:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        # Đường dẫn tới file CSV bên trong container
        self.csv_path = '/opt/airflow/data/uscities.csv'

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def generate_us_states(self):
        """
        Đọc file CSV, lấy các bang duy nhất và chèn vào bảng 'provinces'.
        """
        try:
            # 1. Đọc file CSV
            df_csv = pd.read_csv(self.csv_path)
            
            # 2. Lấy danh sách các bang duy nhất (unique)
            unique_provinces = df_csv['state_name'].unique()
            
            # 3. Chuẩn bị dữ liệu để chèn
            # Chuyển ["Alabama", "Alaska"] thành [("Alabama",), ("Alaska",)]
            province_data = [(name,) for name in unique_provinces]
            
            # 4. Chèn hàng loạt vào DB
            if province_data:
                query = "INSERT INTO provinces (province_name) VALUES %s ON CONFLICT (province_name) DO NOTHING"
                execute_values(self.cur, query, province_data)
                self.conn.commit()
                print(f"Đã chèn thành công {len(province_data)} tỉnh/bang.")
            else:
                print("Không tìm thấy tỉnh/bang nào trong file CSV.")

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo tỉnh/bang từ CSV: {e}")
            raise

def main():
    province_model_generator = Province()
    province_model_generator.generate_us_states()

if __name__ == "__main__":
    main()