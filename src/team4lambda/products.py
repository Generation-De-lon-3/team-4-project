import app
# import json
import pandas as pd


def products():

    connection = app.connection
    cursor = connection.cursor()

    baskets = []

    for item in app.cafe_dict:
        for index, each in enumerate(item['basket']):
            baskets.append(item['basket'][index])

    baskets2 = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in baskets)]
    baskets3 = pd.DataFrame(baskets2)

    products = pd.read_sql_query("SELECT product_size, product_name, product_price FROM products", connection)
    
    # print(baskets3)
    products['product_price'] = products['product_price'].apply(lambda x: "{:.2f}".format(x))
 
    # print(baskets3)
    # print(products)
 
    final = products.merge(baskets3, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
    # print(final)
    
    final = final.to_dict('records')
    
    for item in final:
        cursor.execute(f"INSERT INTO products (product_name, product_size, product_price) VALUES ('{item['product_name']}', '{item['product_size']}', {item['product_price']});")
        connection.commit()

    cursor.close()
    connection.close()


# products()
