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

    def generate_bulk_orders(self, num_orders, execution_date_str=None):
        print(f"Bắt đầu tạo {num_orders} đơn hàng cho ngày {execution_date_str}...")

        if execution_date_str:
            base_date = datetime.strptime(execution_date_str, '%Y-%m-%d')
        else:
            base_date = datetime.now()

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
            print("Không tìm thấy customer hợp lệ.")
            return []

        self.cur.execute("""
            SELECT u.id FROM users u
            JOIN role_user ru ON u.id = ru.user_id
            JOIN roles r ON ru.role_id = r.id
            WHERE r.role_name = 'staff'
        """)
        valid_staffs = [row[0] for row in self.cur.fetchall()]

        self.cur.execute("SELECT id, product_price, product_tax FROM products WHERE product_quantity > 0")
        valid_products = self.cur.fetchall()

        self.cur.execute("SELECT id FROM paymentmethods ORDER BY id ASC")
        payment_ids = [row[0] for row in self.cur.fetchall()]
        # credit card (30%), debit card (5%), paypal (5%), bank transfer (10%), COD (50%)
        weights_payment = [0.30, 0.05, 0.05, 0.10, 0.50] 

        self.cur.execute("SELECT id FROM shippingmethods")
        shipping_methods = [row[0] for row in self.cur.fetchall()]
        
        self.cur.execute("SELECT id FROM orderstatus WHERE order_status_name='Pending'")
        pending_status_id = self.cur.fetchone()[0]

        self.cur.execute("SELECT id FROM paymentstatus WHERE payment_status_name='Pending'")
        default_payment_status_id = self.cur.fetchone()[0]
        
        self.cur.execute("SELECT id FROM shippingstatus WHERE shipping_status_name='Pending'")
        default_shipping_status_id = self.cur.fetchone()[0]

        self.cur.execute("SELECT id, type, value FROM discounts WHERE expired_at > %s", (datetime.now(),))
        valid_discounts = self.cur.fetchall()

        orders_data_list = []
        order_details_temp_storage = []
        
        for _ in range(num_orders):
            customer_id, address_id = random.choice(valid_customers)
            staff_id = random.choice(valid_staffs)
            payment_id = random.choices(payment_ids, weights=weights_payment, k=1)[0]
            shipping_id = random.choice(shipping_methods)
            
            num_prods = random.randint(1, 5)
            selected_prods = random.sample(valid_products, min(num_prods, len(valid_products)))
            
            order_amount = Decimal('0')
            tax_amount = Decimal('0')
            line_items = [] 

            for p_id, p_price, p_tax_rate in selected_prods:
                qty = random.randint(1, 3)
                
                line_amount = p_price * qty
                line_tax = line_amount * (p_tax_rate / Decimal('100'))
                line_subtotal = line_amount + line_tax
                
                order_amount += line_amount
                tax_amount += line_tax
                line_items.append((p_id, qty, p_price, p_tax_rate, line_subtotal))

            discount_id = None
            discount_amount = Decimal('0')
            if random.random() < 0.3 and valid_discounts:
                d = random.choice(valid_discounts)
                discount_id, d_type, d_value = d[0], d[1], d[2]
                if d_type == 'percent':
                    discount_amount = order_amount * (d_value / Decimal('100'))
                else:
                    discount_amount = min(order_amount, d_value)
            
            total_amount = (order_amount - discount_amount) + tax_amount

            if execution_date_str:
                created_at = base_date + timedelta(seconds=random.randint(0, 86399))
            else:
                created_at = datetime.now()
            
            # Gom dữ liệu bảng Orders
            order_tuple = (
                customer_id, staff_id, address_id, order_amount, discount_amount, tax_amount,
                total_amount, discount_id, payment_id, default_payment_status_id, 
                pending_status_id, shipping_id, default_shipping_status_id, created_at
            )
            orders_data_list.append(order_tuple)
            
            order_details_temp_storage.append((len(orders_data_list) - 1, line_items, created_at))

        try:
            self.cur.execute("BEGIN;")

            insert_order_sql = """
                INSERT INTO orders (user_id, staff_id, address_id, order_amount, discount_amount, tax_amount,
                                    total_amount, discount_id, payment_method_id, payment_status_id, 
                                    order_status_id, shipping_method_id, shipping_status_id, created_at)
                VALUES %s RETURNING id
            """
            inserted_order_ids = execute_values(self.cur, insert_order_sql, orders_data_list, fetch=True)
            
            final_details_list = []
            for order_idx, items, c_at in order_details_temp_storage:
                real_order_id = inserted_order_ids[order_idx][0]
                for p_id, qty, price, tax, subtotal in items:
                    final_details_list.append((
                        real_order_id, p_id, qty, price, tax, subtotal, c_at
                    ))

            insert_detail_sql = """
                INSERT INTO orderdetails (order_id, product_id, quantity, product_price, product_tax, subtotal_amount, created_at)
                VALUES %s
            """
            execute_values(self.cur, insert_detail_sql, final_details_list)

            self.conn.commit()
            print(f"Thành công! Đã tạo {len(inserted_order_ids)} đơn hàng.")
            return [row[0] for row in inserted_order_ids]

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi: {e}")
            return []

def main():
    order_registration_model = OrderRegistration()
    order_registration_model.save_order()


if __name__ == "__main__":
    main()
