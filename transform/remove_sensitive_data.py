import pandas as pd
import numpy as np

cafe_file_path = '2021-02-23-isle-of-wight.csv'
df = pd.read_csv(cafe_file_path, na_values='n/a', names=["timestamp of purchase", "location", "customer name", "basket items (name, size and price)", "cash or card payment", "total price", "card number (empty if cash)"])


del df['customer name'] 
#Customer name has been removed



col = 'card number (empty if cash)'
#replace none with nan and select card issuer name
for i in range(len(df[col])):
    current = df[col].iloc[i]
    if df[col][i] == 'None':
        df[col][i] = np.nan
    else:
        df[col][i] = current.split(',')[0]

#sum of null cells is card info
print(df[col].isnull().sum())


print(df['card number (empty if cash)'].head())