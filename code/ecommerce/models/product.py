import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
from ecommerce.config.database import db_config

class Product:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.fake = Faker('vi_VN')

    def __del__(self):
        self.cur.close()
        self.conn.close()

    def get_categories(self):
        self.cur.execute("SELECT id, category_name FROM categories")
        return self.cur.fetchall()

    def get_brand_ids(self):
        self.cur.execute("SELECT id FROM brands")
        return [row[0] for row in self.cur.fetchall()]

    def generate_fake_products(self, num_products=50):
        try:
            categories = self.get_categories()
            brand_ids = self.get_brand_ids()
            
            if not categories:
                print("Chưa có Category. Hãy chạy generate_category trước.")
                return

            # --- KHO DỮ LIỆU TỔ HỢP (COMBINATORIAL DATA) ---
            
            product_recipes = {
                "Điện tử": {
                    "prefixes": ["Thông minh", "Ultra", "Pro", "Gaming", "Không dây", "Di động", "Hiệu năng cao", "Nhỏ gọn"],
                    "bases": ["Điện thoại", "Laptop", "Máy tính bảng", "Màn hình", "Bàn phím", "Chuột", "Tai nghe", "Loa", "Đồng hồ thông minh", "Flycam", "Máy ảnh"],
                    "suffixes": ["X1", "Pro Max", "Lite", "5G", "Thế hệ 2", "RGB", "Elite", "Series 7", "Siêu mỏng", "Chống ồn"],
                    "price": (1250000, 75000000)
                },
                "Thời trang": {
                    "prefixes": ["Vải Cotton", "Dáng ôm", "Cổ điển", "Da thật", "Thường ngày", "Trang trọng", "Mùa hè", "Mùa đông", "Sang trọng", "Thiết kế"],
                    "bases": ["Áo thun", "Quần Jeans", "Áo khoác", "Bộ Vest", "Váy", "Giày Sneakers", "Giày Boots", "Túi xách", "Khăn choàng", "Kính râm", "Đồng hồ"],
                    "suffixes": ["Classic", "Phiên bản giới hạn", "Thoải mái", "Thể thao", "Phong cách phố thị", "Thanh lịch", "Cao cấp", "Xu hướng 2025"],
                    "price": (500000, 12500000)
                },
                "Nhà cửa": {
                    "prefixes": ["Hiện đại", "Bằng gỗ", "Gốm sứ", "Thủy tinh", "Công thái học", "Thông minh", "Tối giản", "Ấm cúng", "Sang trọng"],
                    "bases": ["Ghế Sofa", "Ghế", "Bàn", "Đèn", "Giường", "Thảm", "Rèm cửa", "Kệ sách", "Bình hoa", "Gương", "Bàn làm việc"],
                    "suffixes": ["Phòng khách", "Decor", "Mềm mại", "Có thể điều chỉnh", "Cỡ King", "Bộ", "Sắp xếp"],
                    "price": (750000, 37500000)
                },
                "Làm đẹp": {
                    "prefixes": ["Tự nhiên", "Hữu cơ", "Dạng lì", "Bóng", "Dưỡng ẩm", "Chống lão hóa", "Vitamin C", "Ban đêm", "Hàng ngày"],
                    "bases": ["Son môi", "Kem nền", "Kem dưỡng", "Serum", "Dầu gội", "Nước hoa", "Mặt nạ", "Sữa rửa mặt", "Kem chống nắng"],
                    "suffixes": ["Phục hồi", "Rạng rỡ", "Tinh chất", "No.5", "Chăm sóc", "Trị liệu", "Tái tạo"],
                    "price": (250000, 5000000)
                },
                "Thể thao": {
                    "prefixes": ["Chuyên nghiệp", "Tập luyện", "Ngoài trời", "Chạy bộ", "Siêu bền", "Siêu nhẹ", "Thể thao", "Hiệu suất"],
                    "bases": ["Thảm Yoga", "Tạ tay", "Máy chạy bộ", "Xe đạp", "Vợt Tennis", "Giày chạy bộ", "Túi tập gym", "Bình nước"],
                    "suffixes": ["Thiết bị", "Co giãn", "Tốc độ", "Bền bỉ", "X-Treme", "Năng động"],
                    "price": (375000, 20000000)
                },
                "Đồ chơi": {
                    "prefixes": ["Lego", "Thú bông", "Điều khiển từ xa", "Giáo dục", "Xếp hình", "Hành động", "Board Game", "Sáng tạo"],
                    "bases": ["Xe hơi", "Robot", "Búp bê", "Mô hình", "Bộ trò chơi", "Khối lắp ráp", "Flycam", "Tàu hỏa"],
                    "suffixes": ["Phiêu lưu", "Anh hùng", "Phiên bản sưu tầm", "Gói vui nhộn", "Trẻ em"],
                    "price": (250000, 3750000)
                },
                "Sách": {
                    "prefixes": ["Nghệ thuật của", "Hướng dẫn về", "Lịch sử của", "Hiện đại", "Nâng cao", "Toàn tập", "Hàng ngày", "Bí ẩn của"],
                    "bases": ["Lập trình Python", "Nấu ăn", "Triết học", "Tiểu thuyết", "Kinh tế", "Tâm lý học", "Khoa học", "Thiết kế"],
                    "suffixes": ["Tập 1", "Hướng dẫn cho người mới", "Khóa học chuyên sâu", "Sổ tay", "Bán chạy nhất"],
                    "price": (125000, 2000000)
                },
                "Ô tô": {
                    "prefixes": ["Xe hơi", "Xe máy", "Cao cấp", "Siêu bền", "Di động", "Tự động"],
                    "bases": ["Máy hút bụi", "Giá đỡ điện thoại", "Bọc ghế", "Sáp bóng", "Máy bơm lốp", "Đèn LED", "Bộ dụng cụ"],
                    "suffixes": ["Pro", "360", "Đa năng", "Làm sạch", "Bảo vệ"],
                    "price": (250000, 5000000)
                },
                "Bách hóa": {
                    "prefixes": ["Hữu cơ", "Tươi sống", "Hạng nhất", "Nhập khẩu", "Tự nhiên", "Ăn liền"],
                    "bases": ["Cà phê", "Trà", "Socola", "Dầu Olive", "Mì ống", "Gạo", "Đồ ăn vặt", "Mật ong"],
                    "suffixes": ["Gói", "500g", "1kg", "Hương vị", "Hộp"],
                    "price": (50000, 1250000)
                },
                "Sân vườn": {
                    "prefixes": ["Ngoài trời", "Trong nhà", "Năng lượng mặt trời", "Kim loại", "Nhựa"],
                    "bases": ["Chậu cây", "Xẻng", "Vòi nước", "Đèn", "Ghế", "Bàn", "Đài phun nước"],
                    "suffixes": ["Xanh", "Sân vườn", "Dụng cụ"],
                    "price": (250000, 7500000)
                },
                "Gia dụng": {
                    "prefixes": ["Thông minh", "Kỹ thuật số", "Tự động", "Tiết kiệm năng lượng", "Thép không gỉ"],
                    "bases": ["Tủ lạnh", "Máy giặt", "Lò vi sóng", "Máy xay sinh tố", "Máy nướng bánh mì", "Máy hút bụi", "Nồi chiên không dầu"],
                    "suffixes": ["Inverter", "Cảm ứng", "Pro", "Gia đình"],
                    "price": (1250000, 50000000)
                }
            }
            
            # Recipe mặc định nếu không khớp từ khóa nào
            default_recipe = {
                "prefixes": ["Tiêu chuẩn", "Cao cấp", "Cơ bản", "Mới"],
                "bases": ["Mặt hàng", "Sản phẩm", "Dụng cụ", "Phụ kiện", "Tiện ích"],
                "suffixes": ["Plus", "V2", "Vàng", "Nguyên bản"],
                "price": (250000, 2500000)
            }

            product_data = []
            
            print(f"Bắt đầu tạo {num_products} sản phẩm đa dạng...")

            for _ in range(num_products):
                cat_id, cat_name = random.choice(categories)
                
                recipe = default_recipe
                for key, val in product_recipes.items():
                    if key.lower() in cat_name.lower():
                        recipe = val
                        break
                
                prefix = random.choice(recipe["prefixes"])
                base = random.choice(recipe["bases"])
                suffix = random.choice(recipe["suffixes"])
                
                name_style = random.choice([1, 2, 3])
                if name_style == 1:
                    product_name = f"{prefix} {base} {suffix}"
                elif name_style == 2:
                    product_name = f"{base} {suffix}" 
                else:
                    product_name = f"{prefix} {base}" 

                min_p, max_p = recipe["price"]
                product_price = round(random.uniform(min_p, max_p), 2)
                
                # 1. Kiểm tra các từ khóa cao cấp (Tăng giá 50%)
                premium_keywords = ["Pro", "Ultra", "Luxury", "Elite", "Max", "Sang trọng", "Cao cấp", "Vàng", "Phiên bản giới hạn"]
                if any(x in product_name for x in premium_keywords):
                    product_price = round(product_price * 1.5, -3) # Làm tròn đến hàng nghìn

                # 2. Kiểm tra các từ khóa giá rẻ (Giảm giá 30%)
                budget_keywords = ["Lite", "Mini", "Basic", "Cơ bản", "Nhỏ gọn", "Thường ngày"]
                if any(x in product_name for x in budget_keywords):
                    product_price = round(product_price * 0.7, -3)

                unit_cost = round(product_price * random.uniform(0.6, 0.85), 2)
                product_tax = round(random.uniform(5.0, 12.0), 2)
                product_quantity = random.randint(0, 500) 

                brand_id = random.choice(brand_ids) if brand_ids else None
                
                descriptions = [
                    f"{base} chất lượng cao, phù hợp cho nhu cầu sử dụng hằng ngày.",
                    f"{product_name} với thiết kế hiện đại và công nghệ tiên tiến.",
                    f"Sản phẩm {base} bền bỉ, được nhiều khách hàng tin dùng.",
                    f"Trải nghiệm sự tiện nghi và đẳng cấp cùng {product_name}.",
                    f"Số lượng có hạn cho phiên bản {prefix.lower()}."
                ]
                product_description = random.choice(descriptions)
                
                img_keyword = base.replace(' ', '+')
                product_image_path = f"https://placehold.co/600x400?text={img_keyword}"

                product_data.append((
                    product_name, cat_id, brand_id, product_description,
                    product_price, unit_cost, product_tax, product_quantity, product_image_path
                ))

            query = """INSERT INTO products 
                       (product_name, category_id, brand_id, product_description,
                       product_price, unit_cost, product_tax, product_quantity, product_image_path)
                       VALUES %s"""
            execute_values(self.cur, query, product_data)
            self.conn.commit()
            print(f"Đã tạo thành công {num_products} sản phẩm.")

        except Exception as e:
            self.conn.rollback()
            print(f"Error while generating products: {e}")

def main():
    product_model_generator = Product()
    product_model_generator.generate_fake_products() 

if __name__ == "__main__":
    main()