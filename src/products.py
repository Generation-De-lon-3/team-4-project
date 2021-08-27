import app
import psycopg2


connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()

baskets = []

for item in app.cafe_dict:
        for index, each in enumerate(item['basket']):
            baskets.append(item['basket'][index])

baskets2 = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in baskets)]


val = []

for item in baskets2:
    val.append(f"('{item['product_name']}', '{item['product_size']}', {item['product_price']})")


cursor.execute(f"INSERT INTO products (product_name, product_size, product_price) VALUES {' ,'.join(val)};")
connection.commit()
cursor.close()
connection.close()