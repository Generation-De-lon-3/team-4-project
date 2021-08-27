from numpy import dtype
import remove_sensitive_data as rd
import pandas as pd
import numpy as np
import psycopg2


conn = psycopg2.connect(
    host="localhost",
    database="cafe",
    user="root",
    password="password")

cursor = conn.cursor()
print(('connected'))
#load sql table into df and convert to int object
orders_sql = pd.read_sql_query("SELECT order_id FROM orders", conn, dtype= np.int32)

#add orders_sql order id column to payment
rd.payment_data['order_id'] = orders_sql

#each line of df into a list of dictionary
rd.payment_data = rd.payment_data.to_dict('records')



try:
    cursor.executemany("""INSERT INTO payments(payment_id, payment_method, card_type, total_payment, order_id) VALUES (%(index)s,%(payment_method)s,%(card)s,%(total_price)s,%(order_id)s)""",rd.payment_data)
    conn.commit()
    print('Data Inserted into Database')
    print(cursor.rowcount, "record(s) inserted")
except (Exception, psycopg2.Error) as e:
    conn.rollback()
    print('Exception: ' + str(e))


cursor.close()
conn.close()
