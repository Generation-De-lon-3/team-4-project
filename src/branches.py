import app
import pandas as pd
import psycopg2


connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()


val = []


for each in app.cafe_data:
    if each["branch"] not in val:
        val.append(each["branch"])
    
print(val)

cursor.execute(f"INSERT INTO branches (branch_name) VALUES ('{' ,'.join(val)}');")
connection.commit()
cursor.close()
connection.close()