import pandas as pd
from connection import conn


def payments(data):
    
    connection = conn()
    cursor = connection.cursor()

    cafe = pd.DataFrame(data)
    branches = pd.read_sql_query("SELECT * FROM branches;", connection)
    orders = pd.read_sql_query("SELECT * FROM orders;", connection)
    payments = pd.read_sql_query("SELECT payment_id as order_id FROM payments;", connection)
        
    orders["order_timestamp"] = orders["order_timestamp"].astype(str)
    
    merged = pd.merge(branches, cafe[["order_timestamp", "branch_name", "payment_total", "payment_method"]], on="branch_name", how="right")
                
    orders["x"] = orders.groupby(["order_timestamp", "branch_id"]).cumcount()
    merged["x"] = merged.groupby("order_timestamp").cumcount()
    
    merged = merged.merge(orders, on=("order_timestamp", "branch_id", "x"), how="inner")    
    merged = payments.merge(merged, on=["order_id"], how="right", indicator=True)
    paymentvalues = merged[merged["_merge"] == "right_only"]
            
    sql = "INSERT INTO payments (payment_id, payment_method, payment_total) VALUES (%s, %s, %s)" 
    
    if not paymentvalues.empty:
        for index, row in paymentvalues.iterrows():
            cursor.execute(sql, (row.order_id, row.payment_method, row.payment_total))
            connection.commit()
    cursor.close()
    