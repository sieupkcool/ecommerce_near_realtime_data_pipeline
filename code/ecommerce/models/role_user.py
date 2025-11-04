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
            # --- PHẦN 1: LẤY ID VÀ SỐ LƯỢNG HIỆN TẠI (GIỮ NGUYÊN) ---
            
            # Fetching role IDs for respective roles
            self.cur.execute("SELECT id FROM roles WHERE role_name = 'admin'")
            admin_role_id = self.cur.fetchone()[0]

            self.cur.execute("SELECT id FROM roles WHERE role_name = 'manager'")
            manager_role_id = self.cur.fetchone()[0]

            self.cur.execute("SELECT id FROM roles WHERE role_name = 'staff'")
            staff_role_id = self.cur.fetchone()[0]

            self.cur.execute("SELECT id FROM roles WHERE role_name = 'customer'")
            customer_role_id = self.cur.fetchone()[0]

            # Fetching user IDs from the users table (100 users mới)
            self.cur.execute("SELECT id FROM users WHERE NOT EXISTS"
                             "(SELECT user_id FROM role_user WHERE user_id = users.id)")
            user_ids = [row[0] for row in self.cur.fetchall()]

            # Lấy số lượng TỔNG CỘNG đang có trong CSDL
            self.cur.execute("SELECT count(id) FROM role_user WHERE role_id = %s",(admin_role_id,))
            admin_role_count = self.cur.fetchone()[0]

            self.cur.execute("SELECT count(id) FROM role_user WHERE role_id = %s", (manager_role_id,))
            manager_role_count = self.cur.fetchone()[0]

            self.cur.execute("SELECT count(id) FROM role_user WHERE role_id = %s",(staff_role_id,))
            staff_role_count = self.cur.fetchone()[0]

            # --- PHẦN 2: LOGIC GÁN VAI TRÒ ĐÃ SỬA ---
            
            role_data = []

            for user_id in user_ids:
                # Logic if/else này giờ sẽ kiểm tra VÀ CẬP NHẬT
                # biến đếm ngay trong vòng lặp
                
                if admin_role_count < 1:
                    role_data.append((admin_role_id, user_id))
                    admin_role_count += 1  # <-- SỬA Ở ĐÂY
                elif manager_role_count < 2:
                    role_data.append((manager_role_id, user_id))
                    manager_role_count += 1 # <-- SỬA Ở ĐÂY
                elif staff_role_count < 4:
                    role_data.append((staff_role_id, user_id))
                    staff_role_count += 1 # <-- SỬA Ở ĐÂY
                else:
                    role_data.append((customer_role_id, user_id))
            
            # --- PHẦN 3: CHÈN HÀNG LOẠT (GIỮ NGUYÊN) ---
            query = "INSERT INTO role_user (role_id, user_id) VALUES %s"
            execute_values(self.cur, query, role_data)
            self.conn.commit()

        except Exception as e:
            self.conn.rollback()
            print(f"Error while assigning roles: {e}")


def main():
    role_user_model_generator = RoleUser()
    role_user_model_generator.assign_roles_to_users()


if __name__ == "__main__":
    main()
