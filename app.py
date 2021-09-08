import boto3
import numpy
import pandas as pd
import psycopg2
import conn
import etl


def handler(event, context):
    # Get key and bucket information
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # use boto3 library to get object from S3
    
    # data = s3_object['Body'].read().decode('utf-8')
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    cafe_data = pd.read_csv(s3_object["Body"], sep=",", names=["order_timestamp", "branch_name", "customer", "basket", "payment_total", "payment_method", "card_type"])
    


    
    conn.initdb()
    etl.etl(cafe_data)