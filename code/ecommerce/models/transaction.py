import random

import psycopg2
from ecommerce.models.order import OrderRegistration
from ecommerce.config.database import db_config

from psycopg2.extras import execute_values


class Transaction(object):
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def get_order_data(self, order_id):
        if order_id:
            self.cur.execute("SELECT id, total_amount FROM orders WHERE id = %s", (order_id,))
            order_id, amount = self.cur.fetchone()
            return order_id, amount

        return None, None

    def change_order_status_based_on_transaction_status(self, order_id, status):
        order = OrderRegistration()
        if status: 
            order_status_name = random.choice(['Processing', 'Shipped', 'Delivered'])
            order.change_order_status(order_id, order_status_name)
        else:  
            order_status_name = random.choice(['Pending', 'Cancelled'])
            order.change_order_status(order_id, order_status_name)

    def generate_transaction(self, orderid):
        status = random.choice(['true', 'false'])
        order_id, amount = self.get_order_data(orderid)
        transaction_type = 'payment'
        description = f'Fake transaction description for order #{order_id}'

        self.cur.execute(
            "INSERT INTO transactions (order_id, transaction_type, amount, description, status)"
            "VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (order_id, transaction_type, amount, description, status))

        transaction_id = self.cur.fetchone()[0] if self.cur.rowcount > 0 else None
        self.conn.commit()

        self.change_order_status_based_on_transaction_status(order_id, status)

    def _bulk_change_order_status(self, status_updates):
        """
        Hàm nội bộ để CẬP NHẬT HÀNG LOẠT trạng thái đơn hàng.
        status_updates là một list các tuple: [(order_id, 'Processing'), (order_id, 'Cancelled'), ...]
        """
        
        self.cur.execute("SELECT order_status_name, id FROM orderstatus")
        status_map = dict(self.cur.fetchall()) 
        
        update_data = []
        for order_id, new_status_name in status_updates:
            status_id = status_map.get(new_status_name)
            if status_id:
                update_data.append((status_id, order_id))
                
        if update_data:
            update_query = """
                UPDATE orders AS o
                SET order_status_id = v.status_id
                FROM (VALUES %s) AS v(status_id, order_id)
                WHERE o.id = v.order_id
            """
            execute_values(self.cur, update_query, update_data)
            print(f"Đã cập nhật hàng loạt {len(update_data)} trạng thái đơn hàng.")

    def generate_bulk_transactions(self, order_ids_list):
        """
        Tạo giao dịch và cập nhật trạng thái cho một DANH SÁCH các ID đơn hàng.
        """
        if not order_ids_list:
            print("Không có ID đơn hàng nào để tạo giao dịch.")
            return

        self.cur.execute("SELECT id, total_amount FROM orders WHERE id = ANY(%s)", (order_ids_list,))
        orders_to_process = self.cur.fetchall()
        
        transactions_to_insert = []
        statuses_to_update = []
        
        for order_id, amount in orders_to_process:
            is_success = random.choice([True, False])
            status_str = 'true' if is_success else 'false'
            transaction_type = 'payment'
            description = f'Fake bulk transaction cho đơn hàng #{order_id}'
            
            transactions_to_insert.append((
                order_id, transaction_type, amount, description, status_str
            ))
            
            if is_success:
                statuses_success = ['Processing', 'Shipped', 'Delivered']
                weights_success = [0.10, 0.20, 0.70]
                new_order_status = random.choices(statuses_success, weights=weights_success, k=1)[0]
            else:
                statuses_fail = ['Pending', 'Cancelled']
                weights_fail = [0.20, 0.80]
                new_order_status = random.choices(statuses_fail, weights=weights_fail, k=1)[0]
            
            statuses_to_update.append((order_id, new_order_status))
        
        try:
            self.cur.execute("BEGIN;")
            
            insert_query = """
                INSERT INTO transactions (order_id, transaction_type, amount, description, status)
                VALUES %s
            """
            execute_values(self.cur, insert_query, transactions_to_insert)
            print(f"Đã chèn hàng loạt {len(transactions_to_insert)} giao dịch.")
            
            self._bulk_change_order_status(statuses_to_update)
            
            self.conn.commit()
            
        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo bulk transaction: {e}")


def main():
    transaction = Transaction()
    transaction.generate_transaction(30)


if __name__ == '__main__':
    main()
