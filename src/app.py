import pandas as pd
import json

<<<<<<< HEAD
df = pd.read_csv('2021-02-23-isle-of-wight.csv', names=["timestamp", "branch", "customer", 'basket', "payment_method", "total_price", "card"])
=======

df = pd.read_csv('data/2021-02-23-isle-of-wight.csv', names=["order_timestamp", "branch", "customer", "basket", "payment_method", "payment_total", "card_type"])
>>>>>>> stefan

del df["customer"] 

cafe_dict = df.to_dict('records')



basket_fields = ['product_size', 'product_name', 'product_price']

for item in cafe_dict:
    item['basket'] = item['basket'].split(',')
    item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
        
    for index, each in enumerate(item['basket']):
        item['basket'][index] = each.split(',')

    for index, items in enumerate(item['basket']):  
        item['basket'][index] = dict(zip(basket_fields,items))
<<<<<<< HEAD
        
print(json.dumps(cafe_data, indent=4))
#  [0]['basket'][0]['name'])
=======

    item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())
    
    item["card_type"]= item["card_type"].replace("None", "")
    
    
>>>>>>> stefan
