import app
import psycopg2
import pandas as pd

connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()

branches = pd.read_sql_query("SELECT * FROM branches", connection)

# products = pd.read_sql_query("SELECT * FROM products", connection)


val = []


for each in app.cafe_data:
    for branch in branches.iterrows():
        if branch[1]["branch_name"] == each['branch']:
            branchid = branch[1]["branch_id"]
            val.append(f"('{each['timestamp']}', '{branchid}')")
            

print(len(val))

cursor.execute(f"INSERT INTO orders (timestamp_of_purchase, branch_id) VALUES {' ,'.join(val)};")
connection.commit()
cursor.close()
connection.close()