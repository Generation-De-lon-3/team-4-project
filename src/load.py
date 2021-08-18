import app
import psycopg2

connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()

cursor.execute("INSERT INTO products (product_name, product_size, product_price) VALUES ('a', 'A', 1)")
cursor.close()
connection.close()