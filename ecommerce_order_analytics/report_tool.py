import sys
import os
import sqlite3
import argparse
from datetime import datetime, timedelta

DB_FILE = "ecommerce.db"

def parse_date(date_str):
    try:
        return datetime.strptime(date_str.strip(), "%Y-%m-%d")
    except ValueError:
        print(f"Error: Invalid date format '{date_str}'. Please use YYYY-MM-DD.")
        sys.exit(1)

def get_period_metrics(cursor, start_str, end_str):
    # End date cutoff: include full day up to 23:59:59
    start_dt_str = f"{start_str} 00:00:00"
    end_dt_str = f"{end_str} 23:59:59"
    
    # 1. Total Orders
    cursor.execute("""
        SELECT COUNT(DISTINCT order_id) 
        FROM orders 
        WHERE order_date >= ? AND order_date <= ? AND status != 'CANCELLED'
    """, (start_dt_str, end_dt_str))
    total_orders = cursor.fetchone()[0] or 0
    
    # 2. Total Revenue
    cursor.execute("""
        SELECT SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0))
        FROM order_items oi
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_date >= ? AND o.order_date <= ? AND o.status != 'CANCELLED'
    """, (start_dt_str, end_dt_str))
    res = cursor.fetchone()[0]
    total_revenue = float(res) if res is not None else 0.0
    
    # 3. Unique Customers
    cursor.execute("""
        SELECT COUNT(DISTINCT o.customer_id)
        FROM orders o
        WHERE o.order_date >= ? AND o.order_date <= ? AND o.status != 'CANCELLED' AND o.customer_id != 'C_UNKNOWN'
    """, (start_dt_str, end_dt_str))
    unique_customers = cursor.fetchone()[0] or 0
    
    # 4. Top 3 Products
    cursor.execute("""
        SELECT p.product_name, SUM(oi.quantity * oi.unit_price * (1.0 - oi.discount_percent / 100.0)) AS revenue
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        JOIN orders o ON oi.order_id = o.order_id
        WHERE o.order_date >= ? AND o.order_date <= ? AND o.status != 'CANCELLED'
        GROUP BY p.product_id, p.product_name
        ORDER BY revenue DESC
        LIMIT 3
    """, (start_dt_str, end_dt_str))
    top_products = cursor.fetchall()
    
    return {
        "orders": total_orders,
        "revenue": total_revenue,
        "customers": unique_customers,
        "top_products": top_products
    }

def calc_pct_change(current, previous):
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return ((current - previous) / previous) * 100.0

def format_pct(val):
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.2f}%"

def generate_cli_report(report_type, start_date_str, end_date_str, db_path=DB_FILE):
    if not os.path.exists(db_path):
        print(f"Error: Database file '{db_path}' not found. Please run db_loader.py first.")
        sys.exit(1)
        
    start_dt = parse_date(start_date_str)
    end_dt = parse_date(end_date_str)
    
    if end_dt < start_dt:
        print("Error: End date cannot be before start date.")
        sys.exit(1)
        
    period_days = (end_dt - start_dt).days + 1
    
    # Calculate previous period range
    prev_end_dt = start_dt - timedelta(days=1)
    prev_start_dt = prev_end_dt - timedelta(days=period_days - 1)
    
    prev_start_str = prev_start_dt.strftime("%Y-%m-%d")
    prev_end_str = prev_end_dt.strftime("%Y-%m-%d")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    current_metrics = get_period_metrics(cursor, start_date_str, end_date_str)
    prev_metrics = get_period_metrics(cursor, prev_start_str, prev_end_str)
    
    conn.close()
    
    # % changes
    orders_pct = calc_pct_change(current_metrics["orders"], prev_metrics["orders"])
    revenue_pct = calc_pct_change(current_metrics["revenue"], prev_metrics["revenue"])
    customers_pct = calc_pct_change(current_metrics["customers"], prev_metrics["customers"])
    
    print("\n" + "=" * 65)
    print(f"       E-COMMERCE ORDER ANALYTICS — {report_type.upper()} REPORT")
    print("=" * 65)
    print(f"Current Period : {start_date_str} to {end_date_str} ({period_days} days)")
    print(f"Prior Period   : {prev_start_str} to {prev_end_str} ({period_days} days)")
    print("-" * 65)
    
    print(f"Total Orders     : {current_metrics['orders']:,}  ({format_pct(orders_pct)} vs prior period: {prev_metrics['orders']:,})")
    print(f"Total Revenue    : ${current_metrics['revenue']:,.2f}  ({format_pct(revenue_pct)} vs prior period: ${prev_metrics['revenue']:,.2f})")
    print(f"Unique Customers : {current_metrics['customers']:,}  ({format_pct(customers_pct)} vs prior period: {prev_metrics['customers']:,})")
    
    print("-" * 65)
    print("TOP 3 PRODUCTS BY REVENUE:")
    if current_metrics["top_products"]:
        for idx, (p_name, rev) in enumerate(current_metrics["top_products"], start=1):
            print(f"  {idx}. {p_name:<40} : ${rev:,.2f}")
    else:
        print("  No product sales found for this period.")
    print("=" * 65 + "\n")

def main():
    parser = argparse.ArgumentParser(description="E-Commerce Order Analytics CLI Report Tool")
    parser.add_argument("--type", choices=["daily", "weekly", "monthly"], default="monthly", help="Report type (daily, weekly, monthly)")
    parser.add_argument("--start", help="Start date in YYYY-MM-DD format")
    parser.add_argument("--end", help="End date in YYYY-MM-DD format")
    parser.add_argument("--db", default=DB_FILE, help="Path to SQLite database file")
    
    args = parser.parse_args()
    
    report_type = args.type
    start_date = args.start
    end_date = args.end
    
    if not start_date or not end_date:
        print("\n--- Interactive Date Input ---")
        if not start_date:
            start_date = input("Enter start date (YYYY-MM-DD): ").strip()
        if not end_date:
            end_date = input("Enter end date (YYYY-MM-DD): ").strip()
            
    generate_cli_report(report_type, start_date, end_date, args.db)

if __name__ == "__main__":
    main()
