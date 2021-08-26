import pandas as pd
import numpy as np
import json

from pandas.io import parsers

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
#print(df[col].isnull().sum())

"""print output"""
#print(df.head())

#dataframe to list of dictionaries for each order
cafe_data = df.to_dict('records')



basket_fields = ['size', 'name', 'price']

for item in cafe_data:
    item['basket'] = item['basket'].split(',')
    item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
    

    
    for index, each in enumerate(item['basket']):
        item['basket'][index] = each.split(',')

    for index, items in enumerate(item['basket']):  
        item['basket'][index] = dict(zip(basket_fields,items))

#Replace empty string in size with NaN
for i in range(len(cafe_data)):
    basket = cafe_data[i]['basket']
    for key in basket:
        if key['size'] == "":
            key['size'] = np.nan



#print(json.dumps(cafe_data, indent=4))


# rows list initialization
rows = []

# appending rows
for data in cafe_data:
	data_row = data['basket']
	time = data['timestamp']
	
	for row in data_row:
		row['Time']= time
		rows.append(row)

# row into dataframe basket without quantity
basketdf = pd.DataFrame(rows)

# data for products table no duplicates
productsdf = basketdf.drop_duplicates(subset=['size', 'name', 'price']).reset_index(drop=True)

# cafe data to dataframe
cafe_data_df = pd.DataFrame(cafe_data)

# branch dataframe no duplicates
ranch_data = cafe_data_df['branch'].drop_duplicates().reset_index(drop=True)

# payment dataframe
payment_data = cafe_data_df[['payment_method', 'card', 'total_price']]
payment_data = payment_data.reset_index()
payment_data.index.astype(dtype= 'int64')
payment_data['index'] += 1


# orders dataframe
orders_data = cafe_data_df[['timestamp']]


# basket with quantity
basket_with_quantity = basketdf.pivot_table(index = ['Time', 'name', 'price'], aggfunc ='size')

