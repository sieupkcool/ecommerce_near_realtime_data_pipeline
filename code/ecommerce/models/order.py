import psycopg2
import random
from datetime import datetime, timedelta
from psycopg2.extras import execute_values
from decimal import Decimal
from ecommerce.config.database import db_config


class OrderRegistration(object):

    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def fetch_random_user_by_role(self, role_name):
        self.cur.execute("SELECT id FROM users WHERE id IN (SELECT user_id FROM role_user WHERE role_id IN (SELECT id "
                         "FROM roles WHERE role_name = %s)) ORDER BY RANDOM() DESC LIMIT 1", (role_name,))
        result = self.cur.fetchone()
        return result[0] if result else None

    def fetch_user_address(self, user_id):
        self.cur.execute("SELECT id FROM addresses WHERE user_id = %s", (user_id,))
        result = self.cur.fetchone()
        return result[0] if result else None

    def fetch_random_available_products(self):
        num_of_product = random.randint(1, 10)
        self.cur.execute("SELECT id, product_price, product_tax, product_quantity FROM products "
                         "WHERE product_quantity > 0 ORDER BY "
                         "RANDOM() LIMIT %s", (num_of_product,))
        return self.cur.fetchall()

    def fetch_order_status_item(self, status_name='Pending'):
        self.cur.execute("SELECT * FROM orderstatus WHERE order_status_name=%s LIMIT 1", (status_name,))
        result = self.cur.fetchone()
        return result[0] if result else None

    def fetch_random_order_status_item(self):
        self.cur.execute("SELECT * FROM orderstatus ORDER BY RANDOM() LIMIT 1")
        result = self.cur.fetchone()
        return result[0] if result else None

    def change_order_status(self, order_id, status_name):
        self.cur.execute("SELECT id FROM orderstatus WHERE order_status_name=%s LIMIT 1", (status_name,))
        result = self.cur.fetchone()
        status_id = result[0] if result else None

        if status_id:
            self.cur.execute("""
                UPDATE orders SET order_status_id = %s WHERE id = %s
            """, (status_id, order_id))
            self.conn.commit()
        else:
            return None

    def fetch_random_shipping_method(self):
        self.cur.execute("SELECT id FROM shippingmethods ORDER BY RANDOM() DESC LIMIT 1")
        result = self.cur.fetchone()
        return result[0] if result else None

    def fetch_random_payment_method(self):
        self.cur.execute("SELECT id FROM paymentmethods ORDER BY RANDOM() DESC LIMIT 1")
        result = self.cur.fetchone()
        return result[0] if result else None

    def fetch_order_discount(self):
        self.cur.execute("SELECT id, type, value FROM discounts WHERE expired_at > %s ORDER BY RANDOM() DESC LIMIT 1",
                         (datetime.now(),))
        return self.cur.fetchone()

    def calculate_order_amount(self, products):
        return sum(item[5] for item in products)

    def prepare_order_details_item(self, product):
        for index, item in enumerate(product):
            product_id = item[0]
            order_quantity = max(1, int(item[3] * 0.1))
            product_price = item[1]
            product_tax = item[2]
            product_price_after_tax = product_price * (product_tax / Decimal('100'))
            subtotal_amount = Decimal(product_price_after_tax * order_quantity)

            product[index] = (product_id, order_quantity, product_price, product_tax, subtotal_amount)
        return product

    def save_order(self):

        # for _ in range(num_order):
        order_customer_id = self.fetch_random_user_by_role('customer')
        order_address_id = self.fetch_user_address(order_customer_id)

        print("----------------------------")
        print(order_address_id)
        print("----------------------------")


        if order_customer_id and order_address_id:

            order_staff_id = self.fetch_random_user_by_role('staff')
            order_status_id = self.fetch_order_status_item()
            order_products = self.fetch_random_available_products()
            prepared_order_items = self.prepare_order_details_item(order_products)
            order_amount = sum(item[1] * item[2] for item in prepared_order_items)
            tax_amount = sum(item[2] * (item[3] / Decimal('100')) for item in prepared_order_items)

            if random.randint(0, 10) > 6:
                order_discount = self.fetch_order_discount()
                discount_id = order_discount[0]
                if order_discount[1] == 'percent':
                    discount_amount = order_amount * (order_discount[2] / Decimal('100'))
                else:
                    discount_amount = order_amount - order_discount[2]
            else:
                discount_id = None
                discount_amount = 0

            total_amount = (order_amount + tax_amount) - discount_amount
            order_payment_method = self.fetch_random_payment_method()
            order_shipping_method = self.fetch_random_shipping_method()

            order_data = (
                order_customer_id, order_staff_id, order_address_id, order_amount, discount_amount, tax_amount,
                total_amount,
                discount_id, order_payment_method, None, order_status_id, order_shipping_method, None)

            try:

                self.cur.execute("BEGIN;")

                insert_order_query = """
                    INSERT INTO orders (user_id, staff_id, address_id, order_amount, discount_amount, tax_amount,
                                        total_amount, discount_id, payment_method_id, payment_status_id, order_status_id,
                                        shipping_method_id, shipping_status_id)
                    VALUES %s RETURNING id
                """
                execute_values(self.cur, insert_order_query, [order_data])
                order_id = self.cur.fetchone()[0] if self.cur.rowcount > 0 else None

                # print([order_data])
                print(order_products)

                order_detail_data = [(order_id, *item) for item in prepared_order_items]
                insert_order_details_query = """
                    INSERT INTO orderdetails (order_id, product_id, quantity, product_price, product_tax, subtotal_amount )
                    VALUES %s
                    """
                execute_values(self.cur, insert_order_details_query, order_detail_data)

                print(order_detail_data)

                self.conn.commit()
                print("Simulation completed successfully!")
                return order_id

            except Exception as e:
                self.conn.rollback()
                print(f"Error occurred: {e}")
                return None
        else:
            return None

    def generate_bulk_orders(self, num_orders, execution_date_str=None):
        print(f"Bắt đầu tạo {num_orders} đơn hàng cho ngày {execution_date_str}...")

        if execution_date_str:
            base_date = datetime.strptime(execution_date_str, '%Y-%m-%d')
        else:
            base_date = datetime.now()
        
        # 1. PRE-FETCHING (Lấy dữ liệu 1 lần)
        
        # Lấy (customer_id, address_id) của tất cả customer HỢP LỆ
        self.cur.execute("""
            SELECT u.id, a.id 
            FROM users u
            JOIN addresses a ON u.id = a.user_id
            JOIN role_user ru ON u.id = ru.user_id
            JOIN roles r ON ru.role_id = r.id
            WHERE r.role_name = 'customer'
        """)
        valid_customers = self.cur.fetchall()
        if not valid_customers:
            print("Không tìm thấy customer hợp lệ, không thể tạo đơn hàng.")
            return

        # Lấy tất cả staff
        self.cur.execute("""
            SELECT u.id FROM users u
            JOIN role_user ru ON u.id = ru.user_id
            JOIN roles r ON ru.role_id = r.id
            WHERE r.role_name = 'staff'
        """)
        valid_staffs = [row[0] for row in self.cur.fetchall()]

        # Lấy tất cả sản phẩm (id, giá, thuế)
        self.cur.execute("SELECT id, product_price, product_tax FROM products WHERE product_quantity > 0")
        valid_products = self.cur.fetchall()

        # Lấy các phương thức
        self.cur.execute("SELECT id, payment_method_name FROM paymentmethods")
        payment_methods_data = self.cur.fetchall()
        
        # Tách ID và tên để tạo trọng số
        payment_ids = [row[0] for row in payment_methods_data]
        payment_names = [row[1] for row in payment_methods_data]

        # TẠO TRỌNG SỐ
        # 50% COD, 30% Credit Card, 15% Bank Transfer, 5% PayPal
        weights_payment = [0.30, 0.05, 0.05, 0.10, 0.50] 
        
        # Lấy các phương thức vận chuyển
        self.cur.execute("SELECT id FROM shippingmethods")
        shipping_methods = [row[0] for row in self.cur.fetchall()]
        
        # Lấy các trạng thái mặc định
        self.cur.execute("SELECT id FROM orderstatus WHERE order_status_name='Pending'")
        pending_status_id = self.cur.fetchone()[0]

        self.cur.execute("SELECT id FROM paymentstatus WHERE payment_status_name='Pending'")
        default_payment_status_id = self.cur.fetchone()[0]
        
        self.cur.execute("SELECT id FROM shippingstatus WHERE shipping_status_name='Pending'")
        default_shipping_status_id = self.cur.fetchone()[0]

        # Lấy tất cả mã giảm giá còn hạn
        self.cur.execute("SELECT id, type, value FROM discounts WHERE expired_at > %s", (datetime.now(),))
        valid_discounts = self.cur.fetchall()

        
        # 2. VÒNG LẶP TRONG BỘ NHỚ 
        
        orders_data_list = []
        order_details_data_list = []
        
        for _ in range(num_orders):
            # Lấy ngẫu nhiên từ dữ liệu đã pre-fetch
            customer_id, address_id = random.choice(valid_customers)
            staff_id = random.choice(valid_staffs)
            payment_id = random.choices(payment_ids, weights=weights_payment, k=1)[0]
            shipping_id = random.choice(shipping_methods)
            
            # Chọn sản phẩm cho đơn hàng này
            num_products_in_order = random.randint(1, 5)
            products_for_this_order = random.sample(valid_products, num_products_in_order)
            
            # Tính toán tổng tiền (logic này có thể phức tạp hơn)
            order_amount = sum(p[1] for p in products_for_this_order)
            tax_amount = sum(p[1] * (p[2] / Decimal('100')) for p in products_for_this_order)
            
            # ... (Xử lý discount) ...
            discount_id = None
            discount_amount = Decimal('0')

            # Áp dụng logic 30% đơn hàng có discount
            if random.randint(0, 10) > 7 and valid_discounts: # Chỉ chạy nếu có mã giảm giá
                order_discount = random.choice(valid_discounts)
                discount_id = order_discount[0]
                discount_type = order_discount[1]
                discount_value = order_discount[2]
                
                if discount_type == 'percent':
                    discount_amount = order_amount * (discount_value / Decimal('100'))
                else:
                    # Đảm bảo không giảm giá nhiều hơn số tiền
                    discount_amount = min(order_amount, discount_value)
            
            total_amount = (order_amount + tax_amount) - discount_amount

            # --- LOGIC MỚI: RẢI GIỜ ---
            if execution_date_str:
                # Để đơn hàng rải đều từ 00:00:00 đến 23:59:59 của ngày đó
                random_seconds = random.randint(0, 86399)
                created_at = base_date + timedelta(seconds=random_seconds)
            else:
                # Nếu chạy thật: Dùng thời gian thực
                created_at = datetime.now()
            
            # Lưu dữ liệu đơn hàng
            order_tuple = (
                customer_id, staff_id, address_id, order_amount, discount_amount, tax_amount,
                total_amount, 
                discount_id, 
                payment_id, 
                default_payment_status_id, 
                pending_status_id, 
                shipping_id, 
                default_shipping_status_id,
                created_at
            )
            orders_data_list.append(order_tuple)
            
            # Lưu chi tiết đơn hàng (cần ID đơn hàng, sẽ xử lý sau)
            order_details_data_list.append((len(orders_data_list) - 1, products_for_this_order))

            
        # 3. BULK INSERT (Chèn hàng loạt)
        
        try:
            self.cur.execute("BEGIN;")

            # Chèn 1000 đơn hàng
            insert_order_query = """
                INSERT INTO orders (user_id, staff_id, address_id, order_amount, discount_amount, tax_amount,
                                    total_amount, discount_id, payment_method_id, payment_status_id, order_status_id,
                                    shipping_method_id, shipping_status_id, created_at)
                VALUES %s RETURNING id
            """
            # Chèn và lấy lại 1000 ID đơn hàng đã tạo
            inserted_order_ids = execute_values(self.cur, insert_order_query, orders_data_list, fetch=True)
            
            # Chuẩn bị dữ liệu chi tiết đơn hàng
            final_order_details = []
            for idx, (order_index, products) in enumerate(order_details_data_list):
                order_id = inserted_order_ids[order_index][0]
                for prod in products:
                    prod_id, prod_price, prod_tax = prod
                    quantity = random.randint(1, 3)
                    subtotal = prod_price * quantity
                    
                    final_order_details.append((
                        order_id, prod_id, quantity, prod_price, prod_tax, subtotal, created_at
                    ))

            # Chèn 1000+ chi tiết đơn hàng
            insert_details_query = """
                INSERT INTO orderdetails (order_id, product_id, quantity, product_price, product_tax, subtotal_amount, created_at)
                VALUES %s
            """
            execute_values(self.cur, insert_details_query, final_order_details)

            self.conn.commit()
            print(f"Đã chèn thành công {len(inserted_order_ids)} đơn hàng và {len(final_order_details)} chi tiết đơn hàng.")
            
            # Trả về danh sách ID cho task transaction
            return [row[0] for row in inserted_order_ids]

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi chèn hàng loạt: {e}")
            return []

def main():
    order_registration_model = OrderRegistration()
    order_registration_model.save_order()


if __name__ == "__main__":
    main()
