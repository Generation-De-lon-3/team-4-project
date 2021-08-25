import pandas as pd
import numpy as np
import json

cafe_file_path = '2021-02-23-isle-of-wight.csv'
df = pd.read_csv(cafe_file_path, na_values='n/a', names=["timestamp", "branch", "customer", "basket", "payment_method", "total_price", "card"])


del df['customer'] 
#Customer name has been removed


col = 'card'
#replace none with nan and select card issuer name
for i in range(len(df[col])):
    current = df[col].iloc[i]
    if df[col][i] == 'None':
        df[col][i] = np.nan
    else:
        df[col][i] = current.split(',')[0]

"""sum of null cells is card info"""
print(df[col].isnull().sum())

"""print output"""
print(df.head())

cafe_data = df.to_dict('records')



basket_fields = ['size', 'name', 'price']

for item in cafe_data:
    item['basket'] = item['basket'].split(',')
    item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
    

    
    for index, each in enumerate(item['basket']):
        item['basket'][index] = each.split(',')

    for index, items in enumerate(item['basket']):  
        item['basket'][index] = dict(zip(basket_fields,items))

# print(json.dumps(cafe_data, indent=4))
#Replace empty string in size with NaN
for i in range(len(cafe_data)):
    basket = cafe_data[i]['basket']
    for key in basket:
        if key['size'] == "":
            key['size'] = np.nan



print(json.dumps(cafe_data, indent=4))
