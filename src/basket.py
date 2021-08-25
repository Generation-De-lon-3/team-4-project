import app
import psycopg2
import pandas as pd

connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
cursor = connection.cursor()


column_names = ['product_id', 'product_name', 'product_size', 'product_price']
column_names2 = ['basket_id', 'order_id', 'product_id', 'product_quantity']

cursor.execute("select * from basket")
tupples = cursor.fetchall()

cursor.execute("select * from products")
touples = cursor.fetchall()
cursor.close()

df = pd.DataFrame(tupples, columns=column_names2)
df1 = pd.DataFrame(touples, columns=column_names)

for each in app.cafe_data:
    # print(each)
    for item in each['basket']:
        # print(item)
        for key in item.keys():
            # print(key)
            df['product_id'] = df1['product_name'].apply(lambda x: key.get(x)).fillna('')

    
print(df)



# cursor.execute()
# connection.commit()
# cursor.close()
# connection.close()