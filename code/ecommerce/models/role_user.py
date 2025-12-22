import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config


class RoleUser:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def assign_roles_to_users(self):
        try:
            # Lấy ID các role
            self.cur.execute("SELECT id, role_name FROM roles")
            roles_map = {row[1]: row[0] for row in self.cur.fetchall()}
            
            if not roles_map:
                print("Chưa có Role nào trong bảng roles. Hãy chạy generate_roles trước.")
                return

            # Lấy user chưa có role
            self.cur.execute("""
                SELECT id FROM users 
                WHERE NOT EXISTS (SELECT 1 FROM role_user WHERE user_id = users.id)
            """)
            user_ids = [row[0] for row in self.cur.fetchall()]
            
            if not user_ids:
                print("Tất cả user đều đã có role.")
                return

            print(f"Tìm thấy {len(user_ids)} users chưa có role.")

            # Đếm số lượng hiện tại
            role_counts = {}
            for role_name, role_id in roles_map.items():
                self.cur.execute("SELECT count(*) FROM role_user WHERE role_id = %s", (role_id,))
                role_counts[role_name] = self.cur.fetchone()[0]

            role_data = []
            
            # Logic phân bổ role
            for user_id in user_ids:
                if role_counts.get('admin', 0) < 1:
                    role_data.append((roles_map['admin'], user_id))
                    role_counts['admin'] = role_counts.get('admin', 0) + 1
                elif role_counts.get('manager', 0) < 2:
                    role_data.append((roles_map['manager'], user_id))
                    role_counts['manager'] = role_counts.get('manager', 0) + 1
                elif role_counts.get('staff', 0) < 4:
                    role_data.append((roles_map['staff'], user_id))
                    role_counts['staff'] = role_counts.get('staff', 0) + 1
                else:
                    role_data.append((roles_map['customer'], user_id))
            
            if role_data:
                query = "INSERT INTO role_user (role_id, user_id) VALUES %s ON CONFLICT DO NOTHING"
                execute_values(self.cur, query, role_data)
                self.conn.commit()
                print(f"Đã phân quyền cho {len(role_data)} users.")

        except Exception as e:
            self.conn.rollback()
            print(f"LỖI khi phân quyền: {e}")
            raise e # Báo lỗi cho Airflow

def main():
    role_user_model_generator = RoleUser()
    role_user_model_generator.assign_roles_to_users()


if __name__ == "__main__":
    main()