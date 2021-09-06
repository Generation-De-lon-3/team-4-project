import pandas as pd
import psycopg2
# import products
# import sql
# import json

def app():
    cafe_data = pd.read_csv('data/birmingham_31-08-2021_09-00-00.csv', names=["order_timestamp", "branch", "customer", "basket", "payment_total", "payment_method", "card_type"], na_filter=False)

    cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"])
    cafe_data["payment_method"] = cafe_data["payment_method"].astype(str)
    cafe_data["card_type"] = cafe_data["card_type"].astype(str)
    del cafe_data["customer"] 

    # print(cafe_data.dtypes)

    cafe_dict = cafe_data.to_dict('records')


    basket_fields = ['product_size', 'product_name', 'product_price']

    for item in cafe_dict:
        item['basket'] = item['basket'].replace(" -", "-")
        item['basket'] = item['basket'].replace(", ", ",")

        item['basket'] = item['basket'].split(',')
        # item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
        
        for index, each in enumerate(item['basket']):
            swap = each.rfind(" ")
            swap2 = each.rfind("- ")
            each = each[:swap] + "," + each[swap+1:]
            each = each[:swap2] + "" + each[swap2+1:]
            item['basket'][index] = each.replace(" ", ",", 1)
            # print(each)
            
        # print(item["basket"])
        
        for index, each in enumerate(item['basket']):
            item['basket'][index] = each.split(',')

        for index, items in enumerate(item['basket']):  
            item['basket'][index] = dict(zip(basket_fields, items))
            
        for index, each in enumerate(item["basket"]):
            
            item["basket"][index]["product_name"] = each["product_name"].replace("-", " -")

        # item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())

        item["card_type"] = item["card_type"].replace(".0", "")
        
        item["card_type"] = "".join(['#' for x in item["card_type"][:-4]]) + item["card_type"][-4:]
        
        # item["card_type"]= item["card_type"].replace("None", "")
        
    return cafe_dict
    
    
def connection():
    connection = psycopg2.connect(host="127.0.0.1", user="root", password="password", database="cafe", port=5432)
    return connection

def cafe_dict():
    cafe_dict = app()
    return cafe_dict

# app()