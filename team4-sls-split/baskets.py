import pandas as pd
from connection import conn


def baskets(data):
    
    connection = conn()
    cursor = connection.cursor()

    orders = pd.read_sql_query("SELECT * FROM orders;", connection)
    products = pd.read_sql_query("SELECT * FROM products;", connection)
    baskets = pd.read_sql_query("SELECT order_id FROM baskets;", connection)

    cafe = data
    
    products_dict = products.to_dict("records")

    for each in products_dict:
        for item in cafe:
            for every in item["basket"]:
                if each["product_name"] + each["product_size"] == every["product_name"] + every["product_size"]:
                    every["product_id"] = each["product_id"]

    cafe_data = pd.DataFrame(cafe)

    merged_data = pd.merge(cafe_data, orders, on="order_timestamp")

    merged_dict = merged_data.to_dict('records')  

    final = []
    basketvalues = []    

    for every in merged_dict:
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
