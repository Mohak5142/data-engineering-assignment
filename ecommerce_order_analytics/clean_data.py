import os
import re
import pandas as pd

def clean_orders(df_orders):
    print("Cleaning Orders...")
    issues = {
        "missing_customer_ids": 0,
        "date_format_fixed": 0
    }
    
    # 1. Handle missing customer_ids
    missing_mask = df_orders['customer_id'].isna() | (df_orders['customer_id'].astype(str).str.strip() == '') | (df_orders['customer_id'].astype(str).str.strip() == 'nan')
    issues["missing_customer_ids"] = int(missing_mask.sum())
    df_orders.loc[missing_mask, 'customer_id'] = 'C_UNKNOWN'
    
    # 2. Fix date formats
    original_dates = df_orders['order_date'].astype(str).copy()
    
    # Parse dates using pandas
    parsed_dates = pd.to_datetime(df_orders['order_date'], format='mixed', errors='coerce')
    
    # Identify non-standard string formats (e.g., DD-MM-YYYY)
    # Standard format string check: YYYY-MM-DD HH:MM:SS
    is_standard = original_dates.str.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
    issues["date_format_fixed"] = int((~is_standard).sum())
    
    df_orders['order_date'] = parsed_dates.dt.strftime('%Y-%m-%d %H:%M:%S')
    
    return df_orders, issues

def clean_products(df_products):
    print("Cleaning Products...")
    issues = {
        "product_names_normalized": 0
    }
    
    original_names = df_products['product_name'].astype(str).copy()
    
    # Trim whitespace and convert to Title Case
    normalized_names = original_names.str.strip().str.title()
    
    # Count how many changed
    changed_mask = original_names != normalized_names
    issues["product_names_normalized"] = int(changed_mask.sum())
    
    df_products['product_name'] = normalized_names
    return df_products, issues

def validate_emails(df_customers):
    print("Validating Emails...")
    email_regex = r'^[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}$'
    
    invalid_mask = ~df_customers['email'].astype(str).str.match(email_regex)
    invalid_customers = df_customers.loc[invalid_mask, 'customer_id'].tolist()
    
    issues = {
        "invalid_emails_count": len(invalid_customers),
        "invalid_customer_ids": invalid_customers
    }
    
    # Mark or clean invalid emails
    df_customers.loc[invalid_mask, 'email'] = df_customers.loc[invalid_mask, 'email'] + ".INVALID"
    
    return df_customers, invalid_customers, issues

def check_referential_integrity(df_order_items, df_orders):
    print("Checking Referential Integrity...")
    valid_order_ids = set(df_orders['order_id'].unique())
    
    orphaned_mask = ~df_order_items['order_id'].isin(valid_order_ids)
    orphaned_items = df_order_items[orphaned_mask]
    orphaned_count = len(orphaned_items)
    orphaned_order_ids = sorted(orphaned_items['order_id'].unique().tolist())
    
    issues = {
        "orphaned_order_items_count": orphaned_count,
        "orphaned_order_ids": orphaned_order_ids
    }
    
    # Clean order_items by removing orphaned records
    df_order_items_clean = df_order_items[~orphaned_mask].copy()
    
    return df_order_items_clean, issues

def main():
    os.makedirs("data/cleaned", exist_ok=True)
    
    # Read raw CSVs
    df_orders = pd.read_csv("data/raw/orders.csv", dtype=str)
    df_order_items = pd.read_csv("data/raw/order_items.csv")
    df_products = pd.read_csv("data/raw/products.csv")
    df_customers = pd.read_csv("data/raw/customers.csv")
    
    # Perform cleaning
    df_orders_clean, order_issues = clean_orders(df_orders)
    df_products_clean, product_issues = clean_products(df_products)
    df_customers_clean, invalid_cust_ids, customer_issues = validate_emails(df_customers)
    df_order_items_clean, ref_issues = check_referential_integrity(df_order_items, df_orders_clean)
    
    # Save cleaned files
    df_orders_clean.to_csv("data/cleaned/orders_clean.csv", index=False)
    df_order_items_clean.to_csv("data/cleaned/order_items_clean.csv", index=False)
    df_products_clean.to_csv("data/cleaned/products_clean.csv", index=False)
    df_customers_clean.to_csv("data/cleaned/customers_clean.csv", index=False)
    
    # Write text report
    report_path = "data/cleaned/cleaning_report.txt"
    report_content = f"""==================================================
E-COMMERCE DATA CLEANING REPORT
==================================================

1. ORDERS CLEANING
--------------------------------------------------
- Total raw order rows: {len(df_orders)}
- Orders with missing customer_id imputed ('C_UNKNOWN'): {order_issues['missing_customer_ids']}
- Orders with non-standard date format fixed: {order_issues['date_format_fixed']}

2. PRODUCTS CLEANING
--------------------------------------------------
- Total raw product rows: {len(df_products)}
- Product names normalized (trimmed & title cased): {product_issues['product_names_normalized']}

3. CUSTOMERS EMAIL VALIDATION
--------------------------------------------------
- Total raw customer rows: {len(df_customers)}
- Invalid email addresses found: {customer_issues['invalid_emails_count']}
- Affected Customer IDs: {', '.join(customer_issues['invalid_customer_ids'])}

4. REFERENTIAL INTEGRITY CHECK
--------------------------------------------------
- Total raw order items rows: {len(df_order_items)}
- Orphaned order_items removed (non-existent order_id): {ref_issues['orphaned_order_items_count']}
- Non-existent Order IDs found: {', '.join(map(str, ref_issues['orphaned_order_ids']))}
- Cleaned order items remaining: {len(df_order_items_clean)}

==================================================
Cleaning completed successfully!
"""
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_content)
        
    print(f"\nCleaned files saved to data/cleaned/\nReport generated at {report_path}")
    print(report_content)

if __name__ == "__main__":
    main()
