import boto3
import numpy
import pandas as pd
import psycopg2
from src.team4lambda import branches
from src.team4lambda import products
from src.team4lambda import orders
from src.team4lambda import baskets
from src.team4lambda import payments
import conn
# import sql
# import json


def handler(event, context):
    # # Get key and bucket information
    # key = event['Records'][0]['s3']['object']['key']
    # bucket = event['Records'][0]['s3']['bucket']['name']
    
    # # use boto3 library to get object from S3
    # s3_object = s3.get_object(Bucket=bucket, Key=key)
    
    
    # data = s3_object['Body'].read().decode('utf-8')
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    

    
    
    conn.initdb()
    branches.branches()
    products.products()
    orders.orders()
    payments.payments()
    baskets.baskets()


