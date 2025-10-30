import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
import pandas as pd

class City:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        # Đường dẫn tới file CSV bên trong container
        self.csv_path = '/opt/airflow/data/uscities.csv'

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def generate_cities_for_provinces(self):
        """
        Đọc file CSV và bảng 'provinces', hợp nhất (merge) chúng
        và chèn vào bảng 'cities'.
        """
        try:
            # 1. Đọc file CSV, chỉ lấy các cột cần thiết
            df_cities_csv = pd.read_csv(self.csv_path)
            df_cities_csv = df_cities_csv[['city', 'state_name', 'lat', 'lng']].drop_duplicates()

            # 2. Đọc bảng 'provinces' (đã được tạo ở Bước 2)
            self.cur.execute("SELECT id, province_name FROM provinces")
            provinces_data = self.cur.fetchall()
            
            if not provinces_data:
                print("Lỗi: Bảng 'provinces' trống. Task trước đó có thể đã thất bại.")
                return

            # Chuyển sang DataFrame để merge
            df_provinces_db = pd.DataFrame(provinces_data, columns=['province_id', 'province_name'])

            # 3. Hợp nhất (merge) dữ liệu CSV và dữ liệu DB
            df_merged = pd.merge(
                df_cities_csv, 
                df_provinces_db, 
                left_on='state_name', 
                right_on='province_name'
            )

            # 4. Chuẩn bị dữ liệu để chèn
            # Lấy các cột: city_name, province_id (từ DB), lat, lng
            city_data = list(df_merged[['city', 'province_id', 'lat', 'lng']].itertuples(index=False, name=None))

            # 5. Chèn hàng loạt (bulk-insert) vào database
            if city_data:
                query = ("INSERT INTO cities (city_name, province_id, latitude, longitude)"
                         "VALUES %s ON CONFLICT DO NOTHING")
                
                execute_values(self.cur, query, city_data)
                self.conn.commit()
                print(f"Đã chèn thành công {len(city_data)} thành phố từ file CSV.")
            else:
                print("Không tìm thấy thành phố nào để chèn sau khi merge.")

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo thành phố từ CSV: {e}")
            raise

def main():
    city_model_generator = City()
    city_model_generator.generate_cities_for_provinces()

if __name__ == "__main__":
    main()