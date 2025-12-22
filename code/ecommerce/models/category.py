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
            "Điện tử": ["Điện thoại", "Máy tính xách tay", "Máy tính bảng", "Phụ kiện điện tử"],
            "Thời trang nam": ["Quần áo", "Giày dép", "Đồng hồ", "Phụ kiện nam"],
            "Thời trang nữ": ["Váy đầm", "Túi xách", "Trang sức", "Giày cao gót"],
            "Nhà cửa & Đời sống": ["Nội thất", "Trang trí", "Dụng cụ nhà bếp", "Chăn ga gối đệm"],
            "Sắc đẹp": ["Chăm sóc da", "Trang điểm", "Nước hoa", "Dụng cụ làm đẹp"],
            "Sức khỏe": ["Thực phẩm chức năng", "Dụng cụ y tế", "Sơ cứu"],
            "Thể thao & Dã ngoại": ["Dụng cụ tập gym", "Thể thao đồng đội", "Cắm trại", "Xe đạp"],
            "Đồ chơi & Sở thích": ["Mô hình", "Board Games", "Đồ chơi điện tử"],
            "Mẹ & Bé": ["Tã bỉm", "Xe đẩy", "Đồ dùng cho bé", "Thời trang trẻ em"],
            "Ô tô & Xe máy": ["Phụ kiện ô tô", "Chăm sóc xe", "Phụ kiện xe máy"],
            "Sách": ["Tiểu thuyết", "Sách phi hư cấu", "Giáo dục", "Truyện tranh"],
            "Văn phòng phẩm": ["Dụng cụ viết", "Sản phẩm giấy", "Đồ dùng văn phòng"],
            "Bách hóa": ["Đồ ăn vặt", "Đồ uống", "Gia vị & Nấu ăn"],
            "Thú cưng": ["Thức ăn cho chó", "Thức ăn cho mèo", "Đồ chơi thú cưng"],
            "Thiết bị gia dụng": ["Thiết bị lớn", "Thiết bị nhà bếp nhỏ", "Máy hút bụi"],
            "Sân vườn": ["Cây cảnh", "Dụng cụ làm vườn", "Trang trí ngoài trời"],
            "Nhạc cụ": ["Guitar", "Keyboard", "Trống"],
            "Công cụ & Phần cứng": ["Dụng cụ điện", "Dụng cụ cầm tay", "Đồ bảo hộ"]
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