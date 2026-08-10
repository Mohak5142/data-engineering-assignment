import os
import random
from datetime import datetime, timedelta

def ensure_dirs():
    os.makedirs("data/raw", exist_ok=True)
    os.makedirs("data/cleaned", exist_ok=True)

def generate_customers(num_customers=500):
    first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", 
                   "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
                   "Thomas", "Sarah", "Charles", "Karen", "Christopher", "Nancy", "Daniel", "Lisa",
                   "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
                  "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
                  "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson"]
    domains = ["gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "example.com"]
    customer_types = ["REGULAR", "PREMIUM", "VIP"]

    start_date = datetime(2024, 1, 1)
    
    customers = []
    for i in range(1, num_customers + 1):
        c_id = f"C{1000 + i}"
        fn = random.choice(first_names)
        ln = random.choice(last_names)
        name = f"{fn} {ln}"
        
        # 2% invalid email
        if random.random() < 0.02:
            if random.random() < 0.5:
                email = f"{fn.lower()}{ln.lower()}{domains[0]}" # Missing @
            else:
                email = f"{fn.lower()}.{ln.lower()}@com" # Missing valid domain
        else:
            email = f"{fn.lower()}.{ln.lower()}{i}@{random.choice(domains)}"
            
        reg_date = start_date + timedelta(days=random.randint(0, 500))
        c_type = random.choices(customer_types, weights=[0.6, 0.3, 0.1])[0]
        
        customers.append((c_id, name, email, reg_date.strftime("%Y-%m-%d"), c_type))

    with open("data/raw/customers.csv", "w", encoding="utf-8") as f:
        f.write("customer_id,customer_name,email,registration_date,customer_type\n")
        for row in customers:
            f.write(",".join(map(str, row)) + "\n")
    print(f"Generated {len(customers)} rows for data/raw/customers.csv")
    return customers

def generate_products(num_products=500):
    categories = {
        "Electronics": ["Audio", "Mobile", "Computers", "Wearables", "Gaming"],
        "Clothing": ["Men's Wear", "Women's Wear", "Footwear", "Accessories"],
        "Home": ["Kitchen", "Furniture", "Decor", "Bedding"],
        "Books": ["Fiction", "Non-Fiction", "Sci-Fi", "Biography", "Children"]
    }
    
    products = []
    prod_names_base = [
        "Wireless Headphones", "Smartphone", "Gaming Laptop", "Smart Watch", "Bluetooth Speaker",
        "Cotton T-Shirt", "Denim Jeans", "Running Shoes", "Leather Jacket", "Sunglasses",
        "Coffee Maker", "Ergonomic Chair", "Table Lamp", "Duvet Cover Set", "Wall Clock",
        "Mystery Novel", "Self-Help Guide", "Space Odyssey", "Historical Biography", "Coloring Book"
    ]
    
    for i in range(1, num_products + 1):
        p_id = f"P{1000 + i}"
        cat = random.choice(list(categories.keys()))
        subcat = random.choice(categories[cat])
        base_name = f"{random.choice(prod_names_base)} {i}"
        
        # 10% inconsistent casing or extra whitespace
        r = random.random()
        if r < 0.05:
            p_name = f"  {base_name.upper()}   "
        elif r < 0.10:
            p_name = f" {base_name.lower()} "
        else:
            p_name = base_name.title()
            
        cost_price = round(random.uniform(5.0, 500.0), 2)
        products.append((p_id, p_name, cat, subcat, cost_price))

    with open("data/raw/products.csv", "w", encoding="utf-8") as f:
        f.write("product_id,product_name,category,subcategory,cost_price\n")
        for p_id, p_name, cat, subcat, cp in products:
            # Quote product_name in CSV if needed
            f.write(f'{p_id},"{p_name}",{cat},{subcat},{cp}\n')
    print(f"Generated {len(products)} rows for data/raw/products.csv")
    return products

def generate_orders(customers, num_orders=600):
    statuses = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
    status_weights = [0.15, 0.20, 0.50, 0.05, 0.10]
    regions = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
    
    customer_ids = [c[0] for c in customers]
    start_time = datetime(2024, 6, 1)
    
    orders = []
    valid_order_ids = []
    
    for i in range(1, num_orders + 1):
        o_id = f"O{10000 + i}"
        valid_order_ids.append(o_id)
        
        # 5% missing customer_id
        if random.random() < 0.05:
            c_id = ""
        else:
            c_id = random.choice(customer_ids)
            
        dt = start_time + timedelta(days=random.randint(0, 700), hours=random.randint(0, 23), minutes=random.randint(0, 59), seconds=random.randint(0, 59))
        
        # 5% wrong order_date format (DD-MM-YYYY HH:MM:SS)
        if random.random() < 0.05:
            dt_str = dt.strftime("%d-%m-%Y %H:%M:%S")
        else:
            dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            
        status = random.choices(statuses, weights=status_weights)[0]
        region = random.choice(regions)
        
        orders.append((o_id, c_id, dt_str, status, region))

    with open("data/raw/orders.csv", "w", encoding="utf-8") as f:
        f.write("order_id,customer_id,order_date,status,region_code\n")
        for row in orders:
            f.write(",".join(map(str, row)) + "\n")
    print(f"Generated {len(orders)} rows for data/raw/orders.csv")
    return valid_order_ids

def generate_order_items(valid_order_ids, products, num_items=1200):
    prod_info = {p[0]: p[4] for p in products} # product_id -> cost_price
    prod_ids = list(prod_info.keys())
    
    items = []
    
    for i in range(1, num_items + 1):
        item_id = f"ITM{100000 + i}"
        
        # A small handful (approx 10 rows) with non-existent order_id
        if i <= 10:
            o_id = f"O9999{i}"
        else:
            o_id = random.choice(valid_order_ids)
            
        p_id = random.choice(prod_ids)
        cost_price = prod_info[p_id]
        
        # Unit price is cost_price * markup (e.g. 1.2x to 2.5x)
        unit_price = round(cost_price * random.uniform(1.2, 2.5), 2)
        
        # 3% negative quantity
        if random.random() < 0.03:
            qty = -random.randint(1, 3)
        else:
            qty = random.randint(1, 8)
            
        discount = round(random.choice([0, 0, 0, 5, 10, 15, 20, 25, 50]), 1)
        
        items.append((item_id, o_id, p_id, qty, unit_price, discount))

    with open("data/raw/order_items.csv", "w", encoding="utf-8") as f:
        f.write("item_id,order_id,product_id,quantity,unit_price,discount_percent\n")
        for row in items:
            f.write(",".join(map(str, row)) + "\n")
    print(f"Generated {len(items)} rows for data/raw/order_items.csv")

def main():
    ensure_dirs()
    print("Starting Data Generation...")
    customers = generate_customers(550)
    products = generate_products(550)
    valid_order_ids = generate_orders(customers, 650)
    generate_order_items(valid_order_ids, products, 1300)
    print("Data Generation Complete!")

if __name__ == "__main__":
    main()
