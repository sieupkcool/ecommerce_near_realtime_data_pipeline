import psycopg2
from psycopg2.extras import execute_values
from ecommerce.config.database import db_config
import re

class Category:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def generate_unique_slug(self, category_name, existing_slugs):
        # Tạo slug từ tên
        slug = re.sub(r'\W+', '-', category_name.lower()).strip('-')
        base_slug = slug
        counter = 1
        
        # Nếu slug đã tồn tại thì thêm số
        while slug in existing_slugs:
            slug = f"{base_slug}-{counter}"
            counter += 1
        
        return slug

    def generate_fake_categories(self, num_categories=None):
        ecommerce_categories = {
            "Electronics": ["Smartphones", "Laptops", "Tablets", "Accessories"],
            "Men's Fashion": ["Clothing", "Shoes", "Watches", "Accessories"],
            "Women's Fashion": ["Dresses", "Handbags", "Jewelry", "Shoes"],
            "Home & Living": ["Furniture", "Decoration", "Kitchenware", "Bedding"],
            "Beauty": ["Skincare", "Makeup", "Fragrance", "Tools"],
            "Health": ["Supplements", "Medical Supplies", "First Aid"],
            "Sports & Outdoors": ["Gym Equipment", "Team Sports", "Camping", "Cycling"],
            "Toys & Hobbies": ["Action Figures", "Board Games", "Electronic Toys"],
            "Baby & Kids": ["Diapers", "Baby Gear", "Nursing", "Kids Clothing"],
            "Automotive": ["Car Electronics", "Car Care", "Moto Accessories"],
            "Books": ["Fiction", "Non-Fiction", "Education", "Comics"],
            "Stationery": ["Writing", "Paper Products", "Office Supplies"],
            "Groceries": ["Snacks", "Beverages", "Cooking Essentials"],
            "Pet Supplies": ["Dog Food", "Cat Food", "Pet Toys"],
            "Appliances": ["Large Appliances", "Small Kitchen Appliances", "Vacuums"],
            "Garden": ["Plants", "Gardening Tools", "Outdoor Decor"],
            "Musical Instruments": ["Guitars", "Keyboards", "Drums"],
            "Tools & Hardware": ["Power Tools", "Hand Tools", "Safety Gear"]
        }

        try:
            # Map: name->id để kiểm tra trùng tên
            self.cur.execute("SELECT category_name, id FROM categories")
            existing_map = {row[0]: row[1] for row in self.cur.fetchall()}
            
            # Set: Slug để tạo slug mới không trùng
            self.cur.execute("SELECT slug FROM categories")
            existing_slugs = set(row[0] for row in self.cur.fetchall())

            total_inserted = 0
            print("Bắt đầu đồng bộ danh mục (Idempotent Check)...")

            for parent_name, sub_categories in ecommerce_categories.items():
                
                parent_id = existing_map.get(parent_name)

                if not parent_id:
                    slug_parent = self.generate_unique_slug(parent_name, existing_slugs)
                    
                    insert_query = """
                        INSERT INTO categories (category_name, slug, category_id) 
                        VALUES (%s, %s, NULL) RETURNING id
                    """
                    self.cur.execute(insert_query, (parent_name, slug_parent))
                    parent_id = self.cur.fetchone()[0]
                    
                    existing_map[parent_name] = parent_id
                    existing_slugs.add(slug_parent)
                    total_inserted += 1
                
                child_data_to_insert = []
                for child_name in sub_categories:
                    pass 

                    slug_child_candidate = self.generate_unique_slug(child_name, existing_slugs)
                    
                    self.cur.execute(
                        "SELECT id FROM categories WHERE category_name = %s AND category_id = %s",
                        (child_name, parent_id)
                    )
                    child_exists = self.cur.fetchone()

                    if not child_exists:
                        child_data_to_insert.append((child_name, slug_child_candidate, parent_id))
                        existing_slugs.add(slug_child_candidate)

                if child_data_to_insert:
                    query_child = "INSERT INTO categories (category_name, slug, category_id) VALUES %s"
                    execute_values(self.cur, query_child, child_data_to_insert)
                    total_inserted += len(child_data_to_insert)

            self.conn.commit()
            print(f"Hoàn tất! Số lượng danh mục mới được thêm: {total_inserted}")
            print(f"Các danh mục cũ được giữ nguyên.")

        except Exception as e:
            self.conn.rollback()
            print(f"Lỗi khi tạo categories: {e}")

def main():
    category_model_generator = Category()
    category_model_generator.generate_fake_categories()

if __name__ == "__main__":
    main()