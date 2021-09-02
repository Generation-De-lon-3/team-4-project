import boto3
import pandas as pd
# import json


def handle(event, context):
    # Get key and bucket informaition
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # use boto3 library to get object from S3
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket = bucket, Key = key)
    # data = s3_object['Body'].read().decode('utf-8')
    
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    df = pd.read_csv(s3_object["Body"], sep = ",", names=["order_timestamp", "branch", "customer", "basket", "payment_method", "payment_total", "card_type"])

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

        item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())
        
        item["card_type"]= item["card_type"].replace("None", "")
        
    
    print(cafe_dict)
