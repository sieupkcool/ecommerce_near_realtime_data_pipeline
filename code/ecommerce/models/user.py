from faker import Faker
import psycopg2
from psycopg2.extras import execute_values
import hashlib
from ecommerce.config.database import db_config
import datetime


class User(object):
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.fake = Faker('vi_VN')

    def __del__(self):
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def generate_fake_users(self, num_users=1, execution_date_str=None):
        try:
            user_data = []

            if execution_date_str:
                run_date = datetime.datetime.strptime(execution_date_str, '%Y-%m-%d')
            else:
                run_date = datetime.datetime.now()

            print(f"Bắt đầu tạo {num_users} user cho ngày {run_date}...")

            for _ in range(num_users):
                username = self.fake.user_name()
                password = hashlib.sha256(self.fake.password().encode('utf-8')).hexdigest()
                email = self.fake.email()
                mobile = self.fake.phone_number()
                created_at = self.fake.date_time_between(start_date='-1y', end_date=run_date)
                
                # Đảm bảo dữ liệu không bị None
                if username and email and mobile:
                    user_data.append((username, password, email, mobile, created_at))

            # SỬA 1: Thêm ON CONFLICT DO NOTHING để bỏ qua các dòng trùng lặp (email/username)
            # SỬA 2: Bỏ RETURNING id vì execute_values không xử lý return mặc định
            query = """
                INSERT INTO users (username, password, email, mobile, created_at) 
                VALUES %s 
                ON CONFLICT DO NOTHING
            """
            
            if user_data:
                execute_values(self.cur, query, user_data)
                self.conn.commit()
                print(f"Đã insert thành công {len(user_data)} users (đã trừ các dòng trùng lặp).")
            else:
                print("Không có user nào được tạo.")

        except Exception as e:
            self.conn.rollback()
            print(f"LỖI NGHIÊM TRỌNG khi tạo users: {e}")
            # SỬA 3: Bắt buộc phải RAISE lỗi để Airflow biết mà đánh dấu Failed
            raise e
    
    def has_customer_user(self):
        try:
            query = """
                SELECT 1
                FROM addresses a
                JOIN role_user ru ON a.user_id = ru.user_id
                JOIN roles r ON ru.role_id = r.id
                WHERE r.role_name = 'customer'
                LIMIT 1
            """
            self.cur.execute(query)
            result = self.cur.fetchone()
            return result is not None
        except psycopg2.Error as e:
            print(f"Error while checking customer users: {e}")
            return False

def main():
    user_model_generator = User()
    user_model_generator.generate_fake_users(10)


if __name__ == "__main__":
    main()