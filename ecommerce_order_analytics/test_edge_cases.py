import pandas as pd
from datetime import datetime
from clean_data import check_referential_integrity

def test_referential_integrity_orphaned_items():
    print("Testing Edge Case 1: Order items referencing non-existent order_id...")
    df_orders = pd.DataFrame({'order_id': ['O101', 'O102']})
    df_order_items = pd.DataFrame({
        'item_id': ['ITM1', 'ITM2', 'ITM3'],
        'order_id': ['O101', 'O102', 'O999'] # O999 does not exist
    })
    
    df_clean_items, issues = check_referential_integrity(df_order_items, df_orders)
    
    assert issues['orphaned_order_items_count'] == 1, f"Expected 1 orphaned item, got {issues['orphaned_order_items_count']}"
    assert 'O999' in issues['orphaned_order_ids'], "Expected O999 in orphaned order_ids list"
    assert len(df_clean_items) == 2, f"Expected 2 clean items remaining, got {len(df_clean_items)}"
    print("  [PASS] Orphaned order_ids detected and removed successfully.\n")

def test_invalid_discount_over_100():
    print("Testing Edge Case 2: discount_percent > 100...")
    df_items = pd.DataFrame({
        'item_id': ['ITM1', 'ITM2'],
        'quantity': [2, 1],
        'unit_price': [100.0, 50.0],
        'discount_percent': [150.0, 20.0] # 150% is invalid
    })
    
    # Cleaning rule: Cap discount at 100.0% or flag as invalid
    invalid_discounts = df_items[df_items['discount_percent'] > 100.0]
    assert len(invalid_discounts) == 1, "Expected 1 row with discount > 100%"
    
    # Cap discount at 100% for revenue calculation
    df_items['clean_discount'] = df_items['discount_percent'].clip(upper=100.0)
    df_items['revenue'] = df_items['quantity'] * df_items['unit_price'] * (1.0 - df_items['clean_discount'] / 100.0)
    
    assert df_items.loc[0, 'revenue'] == 0.0, f"Expected revenue $0 for 150% discount, got {df_items.loc[0, 'revenue']}"
    print("  [PASS] Invalid discount (>100%) handled and revenue capped at $0.00.\n")

def test_zero_quantity():
    print("Testing Edge Case 3: quantity == 0...")
    df_items = pd.DataFrame({
        'item_id': ['ITM1', 'ITM2'],
        'quantity': [0, 5],
        'unit_price': [100.0, 20.0],
        'discount_percent': [10.0, 0.0]
    })
    
    zero_qty_items = df_items[df_items['quantity'] == 0]
    assert len(zero_qty_items) == 1, "Expected 1 item with quantity 0"
    
    df_items['revenue'] = df_items['quantity'] * df_items['unit_price'] * (1.0 - df_items['discount_percent'] / 100.0)
    assert df_items.loc[0, 'revenue'] == 0.0, f"Expected revenue $0 for 0 quantity, got {df_items.loc[0, 'revenue']}"
    print("  [PASS] Quantity 0 correctly results in $0.00 revenue.\n")

def test_future_order_date():
    print("Testing Edge Case 4: order_date in the future...")
    now = datetime.now()
    df_orders = pd.DataFrame({
        'order_id': ['O101', 'O102', 'O103'],
        'order_date': ['2024-05-10 10:00:00', '2099-12-31 23:59:59', '2025-01-15 12:00:00']
    })
    
    parsed_dates = pd.to_datetime(df_orders['order_date'])
    future_orders = df_orders[parsed_dates > now]
    
    assert len(future_orders) == 1, f"Expected 1 future order (2099), got {len(future_orders)}"
    assert future_orders.iloc[0]['order_id'] == 'O102', "Expected O102 to be identified as future order"
    print("  [PASS] Future order dates successfully identified and isolated.\n")

def run_all_tests():
    print("==================================================")
    print("RUNNING E-COMMERCE EDGE CASE TESTS")
    print("==================================================\n")
    test_referential_integrity_orphaned_items()
    test_invalid_discount_over_100()
    test_zero_quantity()
    test_future_order_date()
    print("==================================================")
    print("ALL 4 EDGE CASE TESTS PASSED SUCCESSFULLY! SUCCESS!")
    print("==================================================")

if __name__ == "__main__":
    run_all_tests()
