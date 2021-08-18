import pandas as pd


df = pd.read_csv('data/2021-02-23-isle-of-wight.csv', names=["timestamp", "branch", "customer", 'basket', "payment_method", "total_price", "card"])

del df['customer'] 

cafe_data = df.to_dict('records')



basket_fields = ['size', 'name', 'price']

for item in cafe_data:
    item['basket'] = item['basket'].split(',')
    item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
    
# print(cafe_data)
    
    for index, each in enumerate(item['basket']):
        item['basket'][index] = each.split(',')

    for index, items in enumerate(item['basket']):  
        item['basket'][index] = dict(zip(basket_fields,items))

print(cafe_data[0]['basket'][0]['name'])