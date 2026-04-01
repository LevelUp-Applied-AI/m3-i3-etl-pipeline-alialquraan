from sqlalchemy import create_engine, text
import pandas as pd
import os
import json
from datetime import datetime

def create_metadata_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS etl_metadata (
                run_id SERIAL PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                rows_processed INTEGER,
                status TEXT
            )
        """))
        conn.commit()
    print("Metadata table checked/created.")

def extract(engine):
    with engine.connect() as conn:
        last_run = conn.execute(text(
            "SELECT MAX(end_time) FROM etl_metadata WHERE status='SUCCESS'"
        )).scalar()
    
    if last_run:
        order_query = f"SELECT * FROM orders WHERE order_date > '{last_run}'"
        print(f"Incremental Load: Fetching orders newer than {last_run}")
    else:
        order_query = "SELECT * FROM orders"
        print("Full Load: Fetching all orders")
        
    customers = pd.read_sql("SELECT * FROM customers", engine)
    products = pd.read_sql("SELECT * FROM products", engine)
    orders = pd.read_sql(order_query, engine)
    order_items = pd.read_sql("SELECT * FROM order_items", engine)
    
    return {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items
    }

def transform(data_dict):
    cust = data_dict["customers"]
    prod = data_dict["products"]
    ord = data_dict["orders"]
    items = data_dict["order_items"]

    df = items.merge(ord, on="order_id").merge(prod, on="product_id").merge(cust, on="customer_id")
    df['line_total'] = df['quantity'] * df['unit_price']
    
    df = df[df['status'] != 'cancelled']
    df = df[df['quantity'] <= 100]
    
    summary = df.groupby(['customer_id', 'customer_name']).agg(
        total_orders=('order_id', 'nunique'),
        total_revenue=('line_total', 'sum')
    ).reset_index()

    summary['avg_order_value'] = summary['total_revenue'] / summary['total_orders']
    
    if not summary.empty:
        mean_rev = summary['total_revenue'].mean()
        std_rev = summary['total_revenue'].std()
        upper_limit = mean_rev + (3 * std_rev)
        summary['is_outlier'] = summary['total_revenue'] > upper_limit
    else:
        summary['is_outlier'] = False
    
    return summary

def validate(df):

    if df.empty:
        print("Warning: No new data to validate.")
        return {}

    checks = {
        "No Nulls": df[['customer_id', 'customer_name']].notnull().all().all(),
        "Revenue > 0": (df['total_revenue'] > 0).all(),
        "No Duplicates": df['customer_id'].is_unique,
        "Orders > 0": (df['total_orders'] > 0).all()
    }
    
    for check, status in checks.items():
        print(f"Check {check}: {'PASS' if status else 'FAIL'}")
        if not status:
            raise ValueError(f"Critical data quality check failed: {check}")
            
    return checks

def generate_quality_report(df, checks_results):

    report = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records_checked": len(df),
        "checks_summary": {k: "PASS" if v else "FAIL" for k, v in checks_results.items()},
        "flagged_outliers": df[df['is_outlier'] == True][['customer_id', 'total_revenue']].to_dict(orient='records') if 'is_outlier' in df else []
    }
    
    os.makedirs('output', exist_ok=True)
    with open('output/quality_report.json', 'w') as f:
        json.dump(report, f, indent=4)
    print("Quality report generated: output/quality_report.json")

def load(df, engine, csv_path):

    if df.empty:
        print("No new data to load.")
        return
    df.to_sql("customer_analytics", engine, if_exists="replace", index=False)

    os.makedirs('output', exist_ok=True)
    df.to_csv(f"output/{csv_path}", index=False)
    print(f"Successfully loaded {len(df)} rows.")

def main():
    conn_str = "postgresql://postgres:postgres@localhost:5432/amman_market"
    engine = create_engine(conn_str)
    
    create_metadata_table(engine)
    start_time = datetime.now()
    
    print(f"Starting ETL Process at {start_time}...")
    
    try:
        data = extract(engine)
        summary_df = transform(data)
        checks = validate(summary_df)
        
        generate_quality_report(summary_df, checks)
        load(summary_df, engine, "customer_analytics.csv")
        
        status = "SUCCESS"
        rows = len(summary_df)
    except Exception as e:
        print(f"ETL Failed: {e}")
        status = "FAILED"
        rows = 0

    end_time = datetime.now()
    with engine.connect() as conn:
        conn.execute(text("""
            INSERT INTO etl_metadata (start_time, end_time, rows_processed, status)
            VALUES (:start, :end, :rows, :status)
        """), {"start": start_time, "end": end_time, "rows": rows, "status": status})
        conn.commit()
    
    print(f"ETL Pipeline completed with status: {status}")

if __name__ == "__main__":
    main()