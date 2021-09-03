import boto3
import pandas as pd
import psycopg2
import products
import sql
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
        
    
    client = boto3.client('redshift', region_name='eu-west-1')

    REDSHIFT_USER = "awsuser"
    REDSHIFT_CLUSTER = "redshiftcluster-jlqz8zhcuit6"
    REDSHIFT_DATABASE = "team-4-db"
    
    credentials = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600
        )
    
    connection = psycopg2.connect(
        user=credentials['DbUser'], 
        password=credentials['DbPassword'],
        host=REDSHIFT_CLUSTER,
        database=REDSHIFT_DATABASE,   
        port=5439
        )
    
    sql.sql()
    products.products()