import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
from faker import Faker


class Address:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def generate_addresses(self):
        fake = Faker('vi_VN')
        try:
            # Lấy user chưa có địa chỉ
            self.cur.execute("""
                SELECT id FROM users 
                WHERE NOT EXISTS (SELECT 1 FROM addresses WHERE user_id = users.id)
            """)
            user_ids = [row[0] for row in self.cur.fetchall()]

            if not user_ids:
                print("Không tìm thấy user nào thiếu địa chỉ.")
                return

            print(f"Đang tạo địa chỉ cho {len(user_ids)} users...")

            address_data = []
            for user_id in user_ids:
                title = fake.random_element(["Nhà riêng", "Văn phòng"]) # Việt hóa title

                # Lấy 1 thành phố ngẫu nhiên
                self.cur.execute("SELECT id, province_id FROM cities ORDER BY RANDOM() LIMIT 1")
                res = self.cur.fetchone()
                
                if not res:
                    print("Lỗi: Bảng cities trống. Không thể tạo địa chỉ.")
                    return
                
                city_id, province_id = res

                street_address = fake.street_address()
                # Faker vi_VN đôi khi không có secondary_address chuẩn, dùng building number thay thế cho an toàn
                other_address_elements = f"Phòng {fake.building_number()}" 
                full_address = f"{street_address}, {other_address_elements}"

                address_data.append((title, user_id, province_id, city_id, full_address))

            query = """
                INSERT INTO addresses (title, user_id, province_id, city_id, full_address) 
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            execute_values(self.cur, query, address_data)
            self.conn.commit()
            print("Đã tạo xong địa chỉ.")

        except Exception as e:
            self.conn.rollback()
            print(f"LỖI khi tạo địa chỉ: {e}")
            raise e # Báo lỗi cho Airflow


def main():
    address_model_generator = Address()
    address_model_generator.generate_addresses()


if __name__ == "__main__":
    main()