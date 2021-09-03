import et
import psycopg2
import pandas as pd
import app

connection = app.connection

# connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()

branches = pd.read_sql_query("SELECT * FROM branches", connection)

val = []


for each in et.cafe_dict:
    for branch in branches.iterrows():
        if branch[1]["branch_name"] == each['branch']:
            branchid = branch[1]["branch_id"]
            val.append(f"('{each['order_timestamp']}', '{branchid}')")
            

# print(len(val))

cursor.execute(f"INSERT INTO orders (order_timestamp, branch_id) VALUES {' ,'.join(val)};")
connection.commit()
cursor.close()
connection.close()