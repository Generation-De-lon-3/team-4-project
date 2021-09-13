import pandas as pd
import json
import os
from branches import branches
from products import products
from orders import orders
from baskets import baskets
from payments import payments


files=os.listdir("data")
# print(files)

dfs = list()

for file in files:
    df = pd.read_csv(f"data/{file}", sep=",", names=["order_timestamp", "branch_name", "customer", "basket", "payment_total", "payment_method", "card_type"])
    dfs.append(df)
    
def app():
    
    for df in dfs:
        
        cafe_data = df
        
        cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"], format='%d/%m/%Y %H:%M')
        cafe_data["order_timestamp"] = cafe_data["order_timestamp"].astype(str)
        cafe_data["payment_method"] = cafe_data["payment_method"].astype(str)

        del cafe_data["customer"]
        del cafe_data["card_type"]

        cafe_dict = cafe_data.to_dict('records')

        basket_fields = ['product_size', 'product_name', 'product_price']

        for item in cafe_dict:
            item['basket'] = item['basket'].replace(" -", "-")
            item['basket'] = item['basket'].replace(", ", ",")

            item['basket'] = item['basket'].split(',')
            
            for index, each in enumerate(item['basket']):
                swap = each.rfind(" ")
                swap2 = each.rfind("- ")
                each = each[:swap] + "," + each[swap+1:]
                each = each[:swap2] + "" + each[swap2+1:]
                item['basket'][index] = each.replace(" ", ",", 1)            
            
            for index, each in enumerate(item['basket']):
                item['basket'][index] = each.split(',')

            for index, items in enumerate(item['basket']):  
                item['basket'][index] = dict(zip(basket_fields, items))
                
            for index, each in enumerate(item["basket"]):
                item["basket"][index]["product_name"] = each["product_name"].replace("-", " -")
            
        data = json.dumps(cafe_dict)
        data = json.loads(data)

        branches(data)
        orders(data)
        payments(data)
        products(data)
        baskets(data)
app()