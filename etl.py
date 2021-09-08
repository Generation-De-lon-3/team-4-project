import boto3
import numpy
import pandas as pd
from src.team4lambda import branches
from src.team4lambda import products
from src.team4lambda import orders
from src.team4lambda import baskets
from src.team4lambda import payments


def etl(data):

    cafe_data = data
    
    cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"], format='%d/%m/%Y %H:%M')
    cafe_data["payment_method"] = cafe_data["payment_method"].astype(str)
    cafe_data["card_type"] = cafe_data["card_type"].astype(str)
    del cafe_data["customer"] 

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
        
        for index, each in enumerate(item['basket']):
            item['basket'][index] = each.split(',')

        for index, items in enumerate(item['basket']):  
            item['basket'][index] = dict(zip(basket_fields, items))
            
        for index, each in enumerate(item["basket"]):
            item["basket"][index]["product_name"] = each["product_name"].replace("-", " -")

        # item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())

        item["card_type"] = item["card_type"].replace(".0", "")
        item["card_type"] = "".join(['#' for x in item["card_type"][:-4]]) + item["card_type"][-4:]
    
    branches.branches(cafe_dict)
    products.products(cafe_dict)
    orders.orders(cafe_dict)
    payments.payments(cafe_dict)
    baskets.baskets(cafe_dict)
