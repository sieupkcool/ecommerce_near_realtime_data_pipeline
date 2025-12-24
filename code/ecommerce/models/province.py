import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
import pandas as pd

class Province:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.csv_path = '/opt/airflow/data/vncities.csv'

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def has_provinces(self):
        try:
            self.cur.execute("SELECT 1 FROM provinces LIMIT 1")
            exists = self.cur.fetchone() is not None
            return exists
        except psycopg2.Error as e:
            print(f"Error checking cities: {e}")
            self.conn.rollback() 
            return False

    def generate_provinces_for_regions(self):
        try:
            df_provinces_csv = pd.read_csv(self.csv_path)
            df_provinces_csv = df_provinces_csv[['region_name','province_name','latitude','longitude']].drop_duplicates()

            self.cur.execute("SELECT id, region_name FROM regions")
            regions_data = self.cur.fetchall()
            
            if not regions_data:
                print("Lỗi: Bảng 'regions' trống.")
                return

            df_regions_db = pd.DataFrame(regions_data, columns=['region_id', 'region_name'])

            df_merged = pd.merge(
                df_provinces_csv, 
                df_regions_db, 
                left_on='region_name', 
                right_on='region_name'
            )

            province_data = list(df_merged[['province_name', 'region_id', 'latitude', 'longitude']].itertuples(index=False, name=None))

            if province_data:
                query = ("INSERT INTO provinces (province_name, region_id, latitude, longitude)"
                         "VALUES %s ON CONFLICT DO NOTHING")
                
                execute_values(self.cur, query, province_data)
                self.conn.commit()
                print(f"Đã chèn thành công {len(province_data)} tỉnh/thành phố từ file CSV.")
            else:
                print("Không tìm thấy tỉnh/thành phố nào để chèn sau khi merge.")

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo tỉnh/thành phố từ CSV: {e}")
            raise

def main():
    city_model_generator = Province()
    city_model_generator.generate_provinces_for_regions()

if __name__ == "__main__":
    main()