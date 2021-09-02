import ET
import psycopg2
import pandas as pd

connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()


cafe_data=pd.DataFrame(ET.cafe_dict)

cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"])

orders = pd.read_sql_query("SELECT * FROM orders", connection)

val = []

merged_data = pd.merge(cafe_data, orders, on="order_timestamp")

merged_dict = merged_data.to_dict('records')

# print(orders)
# print(cafe_data)
# print(merged_dict)

for each in merged_dict:
    val.append(f"('{each['order_id']}', '{each['payment_method']}', '{each['card_type']}', '{each['payment_total']}')")
    

cursor.execute(f"INSERT INTO payments (payment_id,payment_method, card_type, payment_total) VALUES {' ,'.join(val)};")
connection.commit()
cursor.close()
connection.close()