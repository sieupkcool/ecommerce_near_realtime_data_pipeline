import psycopg2
from psycopg2.extras import execute_values
from faker import Faker
import random
from ecommerce.config.database import db_config

class Product:
    def __init__(self):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.fake = Faker()

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
                "Electronic": {
                    "prefixes": ["Smart", "Ultra", "Pro", "Gaming", "Wireless", "Portable", "High-Performance", "Compact"],
                    "bases": ["Smartphone", "Laptop", "Tablet", "Monitor", "Keyboard", "Mouse", "Headphones", "Speaker", "Smartwatch", "Drone", "Camera"],
                    "suffixes": ["X1", "Pro Max", "Lite", "5G", "Gen 2", "RGB", "Elite", "Series 7", "Ultra Slim", "Noise Cancelling"],
                    "price": (50, 3000)
                },
                "Fashion": {
                    "prefixes": ["Cotton", "Slim-Fit", "Vintage", "Leather", "Casual", "Formal", "Summer", "Winter", "Luxury", "Designer"],
                    "bases": ["T-Shirt", "Jeans", "Jacket", "Suit", "Dress", "Sneakers", "Boots", "Handbag", "Scarf", "Sunglasses", "Watch"],
                    "suffixes": ["Classic", "Limited Edition", "Comfort", "Sport", "Urban Style", "Elegant", "Premium", "Trend 2025"],
                    "price": (20, 500)
                },
                "Home": {
                    "prefixes": ["Modern", "Wooden", "Ceramic", "Glass", "Ergonomic", "Smart", "Minimalist", "Cozy", "Luxury"],
                    "bases": ["Sofa", "Chair", "Table", "Lamp", "Bed", "Rug", "Curtain", "Shelf", "Vase", "Mirror", "Desk"],
                    "suffixes": ["Living", "Deco", "Soft Touch", "Adjustable", "King Size", "Set", "Organizer"],
                    "price": (30, 1500)
                },
                "Beauty": {
                    "prefixes": ["Natural", "Organic", "Matte", "Glossy", "Hydrating", "Anti-Aging", "Vitamin C", "Night", "Daily"],
                    "bases": ["Lipstick", "Foundation", "Cream", "Serum", "Shampoo", "Perfume", "Face Mask", "Cleanser", "Sunscreen"],
                    "suffixes": ["Repair", "Glow", "Essence", "No.5", "Care", "Therapy", "Revitalizing"],
                    "price": (10, 200)
                },
                "Sport": {
                    "prefixes": ["Pro", "Training", "Outdoor", "Running", "Heavy Duty", "Lightweight", "Athletic", "Performance"],
                    "bases": ["Yoga Mat", "Dumbbell", "Treadmill", "Bicycle", "Tennis Racket", "Running Shoes", "Gym Bag", "Water Bottle"],
                    "suffixes": ["Gear", "Flex", "Speed", "Endurance", "X-Treme", "Active"],
                    "price": (15, 800)
                },
                "Toy": {
                    "prefixes": ["Lego", "Plush", "Remote Control", "Educational", "Puzzle", "Action", "Board", "Creative"],
                    "bases": ["Car", "Robot", "Doll", "Figure", "Game Set", "Blocks", "Drone", "Train"],
                    "suffixes": ["Adventure", "Heroes", "Collector Edition", "Fun Pack", "Kids"],
                    "price": (10, 150)
                },
                "Book": {
                    "prefixes": ["The Art of", "Guide to", "History of", "Modern", "Advanced", "Complete", "Daily", "Mystery of"],
                    "bases": ["Python Programming", "Cooking", "Philosophy", "Novel", "Economics", "Psychology", "Science", "Design"],
                    "suffixes": ["Volume 1", "Beginner's Guide", "Masterclass", "Handbook", "Best Seller"],
                    "price": (5, 80)
                },
                "Automotive": {
                    "prefixes": ["Car", "Motorcycle", "Premium", "Heavy Duty", "Portable", "Automatic"],
                    "bases": ["Vacuum Cleaner", "Phone Mount", "Seat Cover", "Wax", "Tire Inflator", "LED Light", "Tool Kit"],
                    "suffixes": ["Pro", "360", "Universal", "Cleaner", "Protection"],
                    "price": (10, 200)
                },
                "Grocer": {
                    "prefixes": ["Organic", "Fresh", "Premium", "Imported", "Natural", "Instant"],
                    "bases": ["Coffee", "Tea", "Chocolate", "Olive Oil", "Pasta", "Rice", "Snacks", "Honey"],
                    "suffixes": ["Pack", "500g", "1kg", "Flavor", "Box"],
                    "price": (2, 50)
                },
                "Garden": {
                    "prefixes": ["Outdoor", "Indoor", "Solar", "Metal", "Plastic"],
                    "bases": ["Plant Pot", "Shovel", "Hose", "Light", "Chair", "Table", "Fountain"],
                    "suffixes": ["Green", "Garden", "Tool"],
                    "price": (10, 300)
                },
                "Appliance": {
                    "prefixes": ["Smart", "Digital", "Automatic", "Energy Saving", "Stainless Steel"],
                    "bases": ["Fridge", "Washing Machine", "Microwave", "Blender", "Toaster", "Vacuum", "Air Fryer"],
                    "suffixes": ["Inverter", "Touch", "Pro", "Home"],
                    "price": (50, 2000)
                }
            }
            
            # Recipe mặc định nếu không khớp từ khóa nào
            default_recipe = {
                "prefixes": ["Standard", "Premium", "Basic", "New"],
                "bases": ["Item", "Product", "Tool", "Accessory", "Gadget"],
                "suffixes": ["Plus", "V2", "Gold", "Original"],
                "price": (10, 100)
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
                
                if any(x in product_name for x in ["Pro", "Ultra", "Luxury", "Elite", "Max"]):
                    product_price = round(product_price * 1.5, 2)
                
                if any(x in product_name for x in ["Lite", "Mini", "Basic"]):
                    product_price = round(product_price * 0.7, 2)

                unit_cost = round(product_price * random.uniform(0.6, 0.85), 2)
                product_tax = round(random.uniform(5.0, 12.0), 2)
                product_quantity = random.randint(0, 500) 

                brand_id = random.choice(brand_ids) if brand_ids else None
                
                descriptions = [
                    f"High quality {base} designed for daily use.",
                    f"The all-new {product_name} features advanced technology.",
                    f"Best in class {base} with durable materials.",
                    f"Experience luxury with this authentic {product_name}.",
                    f"Limited stock available for this {prefix} edition."
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