# E-Commerce Order Analytics System

A comprehensive Python + SQL data pipeline and analytics project designed for processing raw e-commerce data, performing automated cleaning and validation, running advanced SQL analytical queries in SQLite, providing a standard-library CLI report tool, and verifying edge-case behaviors.

---

## 📁 Project Structure

```
ecommerce_order_analytics/
├── data/
│   ├── raw/
│   │   ├── orders.csv            # Raw orders dataset (650+ rows)
│   │   ├── order_items.csv       # Raw order items dataset (1300+ rows)
│   │   ├── products.csv          # Raw products dataset (550+ rows)
│   │   └── customers.csv         # Raw customers dataset (550+ rows)
│   └── cleaned/
│       ├── orders_clean.csv      # Cleaned orders dataset
│       ├── order_items_clean.csv # Cleaned order items dataset
│       ├── products_clean.csv    # Cleaned products dataset
│       ├── customers_clean.csv   # Cleaned customers dataset
│       └── cleaning_report.txt   # Detailed report of issues found and fixed
├── generate_data.py              # Part 1: Synthetic data generator
├── clean_data.py                 # Part 2: Pandas data cleaning & validation pipeline
├── queries.sql                   # Part 3: 16 analytical SQL queries (Basic, Intermediate, Advanced)
├── db_loader.py                  # Part 3: SQLite schema setup, DB loader & query runner
├── report_tool.py                # Part 4: Standard library CLI report tool
├── test_edge_cases.py            # Part 5: Edge-case unit test suite (assert-based)
└── README.md                     # Documentation
```

---

## 🛠️ Requirements & Installation

- **Python 3.8+**
- **Pandas** (`pip install pandas`)

No additional external dependencies are required. `sqlite3` and `argparse` are part of the Python standard library.

---

## 🚀 Execution Guide (Run in Order)

### Step 1: Generate Synthetic Raw Data
Generates raw CSV files containing intentional data quality anomalies (missing customer IDs, non-standard date formats, un-trimmed product strings, invalid emails, and orphaned order IDs).
```bash
python generate_data.py
```

### Step 2: Clean & Validate Data
Parses date formats, imputes missing customer IDs, normalizes product names, validates email addresses, removes orphaned order items, and outputs `cleaning_report.txt`.
```bash
python clean_data.py
```

### Step 3: Load into SQLite & Run SQL Queries
Initializes `ecommerce.db`, loads cleaned datasets into SQLite tables with primary/foreign key schemas, and executes all 16 SQL queries defined in `queries.sql`.
```bash
python db_loader.py
```

### Step 4: Run the CLI Report Tool
Generates formatted analytics reports (total orders, total revenue, unique customers, top 3 products, and percentage change vs. prior period of equal length).

**Example CLI usage:**
```bash
python report_tool.py --start 2025-01-01 --end 2025-01-31 --type monthly
```

**Interactive usage:**
```bash
python report_tool.py
```

### Step 5: Execute Edge Case Tests
Runs assertion tests for orphaned items, invalid discount percentages, zero quantities, and future order dates.
```bash
python test_edge_cases.py
```

---

## 📊 Summary of SQL Analytical Queries (`queries.sql`)

### Basic
1. **Total Revenue per Category**: Categorical revenue aggregated across non-cancelled orders.
2. **Top 10 Customers**: Rank customers by overall order spend.
3. **Monthly Order Volume**: Order trends for the preceding 12 months.

### Intermediate
4. **Customers without Delivered Orders**: Customers who ordered but never received a delivered item.
5. **Products with High Return Rates**: Products where return quantity exceeds purchase quantity.
6. **Category Return Rates**: Percentage of items returned per category.

### Advanced
7. **Regional Running Revenue**: Cumulative window sum (`SUM() OVER`) of daily revenue per region.
8. **Product Category Ranking**: `DENSE_RANK()` by revenue within each product category.
9. **Order Gap & At-Risk Customer Flag**: `LAG()` window function calculating inter-order days, flagging average gap > 30 days as "At Risk".
10. **Multi-Level CTE Spending Tiers**: Monthly customer revenue categorized into High/Medium/Low tiers and aggregated by month.
11. **Customer LTV Quartiles**: `NTILE(4)` dividing customers into Platinum, Gold, Silver, and Bronze tiers.
12. **Year-over-Year (YoY) Comparison**: Monthly revenue compared against the matching month of the prior year.
13. **Customer Category Shift**: `FIRST_VALUE()` vs `LAST_VALUE()` tracking whether a customer switched product categories over time.
14. **Cumulative Revenue Distribution**: Pareto analysis tracking % of total revenue driven by top N% of customers.
15. **Cohort Analysis & Retention**: Tracks customer retention across months 0, 1, 2, 3 following registration month.
16. **Frequently Bought Together**: Self-join on `order_items` discovering product co-purchase pairs.
