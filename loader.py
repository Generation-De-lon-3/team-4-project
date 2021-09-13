import boto3
import json
from connection import initdb
from branches import branches
from products import products
from orders import orders
from baskets import baskets
from payments import payments

def handle(event, handler):
        
    data = json.loads(event["Records"][0]["body"])
    
    initdb()
    branches(data)
    orders(data)
    payments(data)
    products(data)
    baskets(data)