import psycopg2
import pandas as pd



def orders(data):
    connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
    cursor = connection.cursor()

    cafe = pd.DataFrame(data)
    branches = pd.read_sql_query("SELECT * FROM branches", connection)
    orders = pd.read_sql_query("SELECT order_timestamp, branch_id FROM orders;", connection)

    
    orders["order_timestamp"] = orders["order_timestamp"].astype(str)

    
    merged = pd.merge(branches, cafe[["order_timestamp", "branch_name"]], on="branch_name", how="right") 
    merged = orders.merge(merged, on=["order_timestamp", "branch_id"], how="right", indicator=True)
    ordervalues = merged[merged["_merge"] == "right_only"]
                    
    sql = "INSERT INTO orders (order_timestamp, branch_id) VALUES (%s, %s)"

    if not ordervalues.empty:
        for index, row in ordervalues.iterrows():
            cursor.execute(sql, (row.order_timestamp, row.branch_id))
            connection.commit()
    cursor.close()