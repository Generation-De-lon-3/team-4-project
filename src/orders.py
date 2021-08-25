import app
import psycopg2

connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()

val = []

for each in app.cafe_data:
    val.append(f"('{each['name']}', '{item['size']}', {item['price']})")



# cursor.execute()
# connection.commit()
# cursor.close()
# connection.close()