import pandas as pd
import conn


def orders(data):
    connection = conn.connection()
    cursor = connection.cursor()

    branches = pd.read_sql_query("SELECT * FROM branches", connection)
    orders = pd.read_sql_query("SELECT order_timestamp, branch_id FROM orders;", connection)

    cafe = pd.DataFrame(data)
    
    merged = pd.merge(branches, cafe[["order_timestamp", "branch_name"]], on="branch_name", how="right")
    
    merged = orders.merge(merged.drop_duplicates(), on=["branch_id", "order_timestamp"], how="right", indicator=True)
    values = merged[merged["_merge"] == "right_only"]
        
    sql = "INSERT INTO orders (order_timestamp, branch_id) VALUES (%s, %s)"

    if not values.empty:
        for index, row in values.iterrows():
            cursor.execute(sql, (row.order_timestamp, row.branch_id))
            connection.commit()
    cursor.close()
