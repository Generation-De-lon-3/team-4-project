import pandas as pd
from connection import conn


def products(data):
    
    connection = conn()
    cursor = connection.cursor()
    
    basket = []
    
    for item in data:
        for index, each in enumerate(item['basket']):
            basket.append(item['basket'][index])
            
    basket2 = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in basket)]
    basket3 = pd.DataFrame(basket2)
    
    products = pd.read_sql_query("SELECT product_size, product_name, product_price FROM products;", connection)
    products['product_price'] = products['product_price'].apply(lambda x: "{:.2f}".format(x))
    
    final = products.merge(basket3, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']    
    productvalues = final.to_dict('records')
    
    for item in productvalues:
        cursor.execute(f"INSERT INTO products (product_name, product_size, product_price) VALUES ('{item['product_name']}', '{item['product_size']}', {item['product_price']});")
        connection.commit()
    cursor.close()
    connection.close()
    