import pandas as pd
import conn
# import etl

def orders(data):
    connection = conn.connection()
    cursor = connection.cursor()

    branches = pd.read_sql_query("SELECT * FROM branches", connection)
    orders = pd.read_sql_query("SELECT order_timestamp, branch_id FROM orders;", connection)
    
    # orders["order_timestamp"] = orders["order_timestamp"].astype(str)


    cafe = pd.DataFrame(data)
    
    values = []
    
    merged = pd.merge(branches, cafe["branch_name"], on="branch_name", how="right")
    merged = pd.merge(orders, cafe[["order_timestamp", "branch_name"]], on="order_timestamp", how="right")
    
    merged["branch_id"] = merged["branch_id"].astype("Int64")
 
    
    final = merged[merged['branch_id'].isnull()]
    
        
    for index in final.index:
        branch_name = final["branch_name"][index]
        orderid = branches.loc[branches["branch_name"] == branch_name, "branch_id"].iloc[0]
        values.append(f"('{final['order_timestamp'][index]}', '{orderid}')")
        

    if values:
        cursor.execute(f"INSERT INTO orders (order_timestamp, branch_id) VALUES {' ,'.join(values)};")
        connection.commit()
    cursor.close()
    connection.close()
