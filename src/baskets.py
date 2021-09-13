import psycopg2
import pandas as pd



def baskets(data):
    
    connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
    cursor = connection.cursor()


    orders = pd.read_sql_query("SELECT * FROM orders;", connection)
    products = pd.read_sql_query("SELECT * FROM products;", connection)
    baskets = pd.read_sql_query("SELECT order_id FROM baskets;", connection)
    orders["order_timestamp"] = orders["order_timestamp"].astype(str)
    branches = pd.read_sql_query("SELECT * FROM branches;", connection)

    cafe_data = pd.DataFrame(data)
    cafe = data
    
    products_dict = products.to_dict("records")

    for each in products_dict:
        for item in cafe:
            for every in item["basket"]:
                if each["product_name"] + each["product_size"] == every["product_name"] + every["product_size"]:
                    every["product_id"] = each["product_id"]

    merged = pd.merge(branches, cafe_data, on="branch_name", how="right") 
    
    orders["x"] = orders.groupby(["order_timestamp", "branch_id"]).cumcount()
    merged["x"] = merged.groupby("order_timestamp").cumcount()
    
    merged = merged.merge(orders, on=("order_timestamp", "x", "branch_id"), how="inner")
    merged = merged.to_dict('records')  

    final = []
    basketvalues = []    

    for every in merged:
        for entry in every["basket"]:
            if every["order_id"] in baskets.values:
                continue
            final.append(f"{every['order_id']}, {entry['product_id']}")
            
    for entry in [*{*final}]:
        entry = f"({entry}, " + f"{str(final.count(entry))})"
        basketvalues.append(entry)
        
    if basketvalues:
        cursor.execute(f"INSERT INTO baskets (order_id, product_id, product_quantity) VALUES {' ,'.join(basketvalues)};")
        connection.commit()
    cursor.close()
    connection.close()