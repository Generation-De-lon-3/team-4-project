import boto3
from connection import initdb
from branches import branches
from products import products
from orders import orders
from baskets import baskets
from payments import payments

def handle(event, handler):
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    
    # data = 
        
    
    initdb()
    branches(data)
    products(data)
    orders(data)
    payments(data)
    baskets(data)
