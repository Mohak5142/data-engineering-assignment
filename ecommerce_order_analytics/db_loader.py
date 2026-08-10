import os
import sqlite3
import pandas as pd

DB_PATH = "ecommerce.db"

def init_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON;")
    
    # Create tables
    cursor.execute("""
    CREATE TABLE customers (
        customer_id TEXT PRIMARY KEY,
        customer_name TEXT NOT NULL,
        email TEXT,
        registration_date TEXT,
        customer_type TEXT
    );
    """)
    
    cursor.execute("""
    CREATE TABLE products (
        product_id TEXT PRIMARY KEY,
        product_name TEXT NOT NULL,
        category TEXT,
        subcategory TEXT,
        cost_price REAL
    );
    """)
    
    cursor.execute("""
    CREATE TABLE orders (
        order_id TEXT PRIMARY KEY,
        customer_id TEXT,
        order_date TEXT,
        status TEXT,
        region_code TEXT,
        FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
    );
    """)
    
    cursor.execute("""
    CREATE TABLE order_items (
        item_id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        product_id TEXT NOT NULL,
        quantity INTEGER,
        unit_price REAL,
        discount_percent REAL,
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        FOREIGN KEY (product_id) REFERENCES products (product_id)
    );
    """)
    
    conn.commit()
    conn.close()
    print(f"Database schema initialized in {DB_PATH}")

def load_data():
    conn = sqlite3.connect(DB_PATH)
    
    df_customers = pd.read_csv("data/cleaned/customers_clean.csv")
    df_products = pd.read_csv("data/cleaned/products_clean.csv")
    df_orders = pd.read_csv("data/cleaned/orders_clean.csv")
    df_order_items = pd.read_csv("data/cleaned/order_items_clean.csv")
    
    df_customers.to_sql("customers", conn, if_exists="append", index=False)
    df_products.to_sql("products", conn, if_exists="append", index=False)
    df_orders.to_sql("orders", conn, if_exists="append", index=False)
    df_order_items.to_sql("order_items", conn, if_exists="append", index=False)
    
    conn.close()
    print("Data loaded successfully into SQLite database tables!")

def execute_queries():
    print("\nExecuting all 16 SQL queries from queries.sql...\n")
    conn = sqlite3.connect(DB_PATH)
    
    with open("queries.sql", "r", encoding="utf-8") as f:
        sql_content = f.read()
        
    for i in range(1, 17):
        start_marker = f"-- QUERY_{i}_START"
        end_marker = f"-- QUERY_{i}_END"
        
        if start_marker in sql_content and end_marker in sql_content:
            query_sql = sql_content.split(start_marker)[1].split(end_marker)[0].strip()
            print(f"{'='*70}\nQUERY {i}\n{'='*70}")
            try:
                df_result = pd.read_sql_query(query_sql, conn)
                print(df_result.head(10).to_string(index=False))
                if len(df_result) > 10:
                    print(f"... ({len(df_result)} total rows returned)")
            except Exception as e:
                print(f"ERROR executing Query {i}: {e}")
            print("\n")
            
    conn.close()

def main():
    init_db()
    load_data()
    execute_queries()

if __name__ == "__main__":
    main()
