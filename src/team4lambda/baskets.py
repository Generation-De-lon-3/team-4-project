import pandas as pd
import app


def baskets():

    connection = app.connection
    cursor = connection.cursor()


    orders = pd.read_sql_query("SELECT * FROM orders", connection)
    products = pd.read_sql_query("SELECT * FROM products", connection)
    baskets = pd.read_sql_query("SELECT order_id FROM baskets", connection)


    products_dict = products.to_dict("records")

    for each in products_dict:
        for item in app.cafe_dict:
            for every in item["basket"]:
                if each["product_name"] + each["product_size"] == every["product_name"] + every["product_size"]:
                    every["product_id"] = each["product_id"]
                    
    cafe_data = pd.DataFrame(app.cafe_dict)
    # cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"])

    merged_data = pd.merge(cafe_data, orders, on="order_timestamp")

    merged_dict = merged_data.to_dict('records')  


    for every in merged_dict:
        for entry in every["basket"]:
            if every["order_id"] in baskets.values:
                continue
            cursor.execute(f"INSERT INTO baskets (order_id, product_id, product_quantity) VALUES ('{every['order_id']}', '{entry['product_id']}', '1') ON CONFLICT (order_id, product_id) DO UPDATE SET product_quantity = baskets.product_quantity + 1;")
            connection.commit()   

        
    cursor.close()
    connection.close()

# baskets()