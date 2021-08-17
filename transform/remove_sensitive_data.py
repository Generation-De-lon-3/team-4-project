import pandas as pd
import numpy as np

cafe_file_path = '2021-02-23-isle-of-wight.csv'
df = pd.read_csv(cafe_file_path, na_values='n/a', names=["timestamp of purchase", "location", "customer name", "basket items (name, size and price)", "cash or card payment", "total price", "card number (empty if cash)"])

#print(df.head())
del df['customer name'] 
print(df.head())
#print(df['card number (empty if cash)'][1][0:8].rstrip())


col = 'card number (empty if cash)'

for i in range(len(df[col])):
    current = df[col].iloc[i]
    if df[col][i] == 'None':
        df[col][i] = np.nan
    else:
        df[col][i] = current.split(',')[0]

print(current)
print(df[col].isnull().sum())


print(df['card number (empty if cash)'].head())