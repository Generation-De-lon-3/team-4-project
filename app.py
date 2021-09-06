import boto3
import numpy
import pandas as pd
import psycopg2
import branches
import products
import orders
import baskets
import payments
# import sql
# import json


def handle(event, context):
    # # Get key and bucket information
    # key = event['Records'][0]['s3']['object']['key']
    # bucket = event['Records'][0]['s3']['bucket']['name']
    
    # # use boto3 library to get object from S3
    s3 = boto3.client('s3')
    # s3_object = s3.get_object(Bucket=bucket, Key=key)
    
    
    # data = s3_object['Body'].read().decode('utf-8')
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    
    s3_object = s3.get_object(Bucket="delon3-team-4-bucket", Key="2021/8/31/birmingham_31-08-2021_09-00-00.csv")

    
    df = pd.read_csv(s3_object["Body"], sep=",", names=["order_timestamp", "branch", "customer", "basket", "payment_total", "payment_method", "card_type"])


    client = boto3.client('redshift', region_name='eu-west-1')

    REDSHIFT_USER = "awsuser"
    REDSHIFT_CLUSTER = "redshiftcluster-jlqz8zhcuit6"
    REDSHIFT_HOST = "redshiftcluster-jlqz8zhcuit6.cc3hslvy2bfm.eu-west-1.redshift.amazonaws.com",
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
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,   
        port=5439
        )
    
    cursor = connection.cursor()
    
    # ---------------------------------------------------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cafe.products (
        product_id INT IDENTITY (1,1) NOT NULL,
        product_name varchar(100) NOT NULL,
        product_size varchar(10),
        product_price float NOT NULL,
        UNIQUE (product_name, product_size, product_price),
        PRIMARY KEY (product_id)
        );

        CREATE TABLE IF NOT EXISTS cafe.branches (
            branch_id INT IDENTITY (1,1) NOT NULL,
            branch_name varchar(22) NOT NULL,
            UNIQUE (branch_name),
        PRIMARY KEY (branch_id)
        );

        CREATE TABLE IF NOT EXISTS cafe.orders (
            order_id INT IDENTITY (1,1) NOT NULL,
            order_timestamp timestamp NOT NULL,
            branch_id int NOT NULL,
            FOREIGN KEY (branch_id) REFERENCES branches (branch_id),
            UNIQUE (order_timestamp, branch_id),
        PRIMARY KEY (order_id)
        );

        CREATE TABLE IF NOT EXISTS cafe.baskets (
            order_id int NOT NULL,
            product_id int NOT NULL,
            product_quantity int NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (product_id),
            FOREIGN KEY (order_id) REFERENCES orders (order_id),
            UNIQUE (order_id, product_id),
        PRIMARY KEY (order_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS cafe.payments (
            payment_id int NOT NULL,
            payment_method varchar(10) NOT NULL,
            card_type varchar(22),
            payment_total float NOT NULL,
            FOREIGN KEY (payment_id) REFERENCES orders (order_id),
            UNIQUE (payment_id),
        PRIMARY KEY (payment_id)
        );
    """)

    cafe_data = pd.read_csv('data/birmingham_31-08-2021_09-00-00.csv', names=["order_timestamp", "branch", "customer", "basket", "payment_total", "payment_method", "card_type"], na_filter=False)

    cafe_data["order_timestamp"] = pd.to_datetime(cafe_data["order_timestamp"])
    cafe_data["payment_method"] = cafe_data["payment_method"].astype(str)
    cafe_data["card_type"] = cafe_data["card_type"].astype(str)
    del cafe_data["customer"] 

    # print(cafe_data.dtypes)

    cafe_dict = cafe_data.to_dict('records')


    basket_fields = ['product_size', 'product_name', 'product_price']

    for item in cafe_dict:
        item['basket'] = item['basket'].replace(" -", "-")
        item['basket'] = item['basket'].replace(", ", ",")

        item['basket'] = item['basket'].split(',')
        # item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
        
        for index, each in enumerate(item['basket']):
            swap = each.rfind(" ")
            swap2 = each.rfind("- ")
            each = each[:swap] + "," + each[swap+1:]
            each = each[:swap2] + "" + each[swap2+1:]
            item['basket'][index] = each.replace(" ", ",", 1)
            # print(each)
            
        # print(item["basket"])
        
        for index, each in enumerate(item['basket']):
            item['basket'][index] = each.split(',')

        for index, items in enumerate(item['basket']):  
            item['basket'][index] = dict(zip(basket_fields, items))
            
        for index, each in enumerate(item["basket"]):
            
            item["basket"][index]["product_name"] = each["product_name"].replace("-", " -")

        # item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())

        item["card_type"] = item["card_type"].replace(".0", "")
        
        item["card_type"] = "".join(['#' for x in item["card_type"][:-4]]) + item["card_type"][-4:]
        
        # item["card_type"]= item["card_type"].replace("None", "")
        
    # print(json.dumps(cafe_dict, indent=2))
    
    branches.branches()
    products.products()
    orders.orders()
    payments.payments()
    baskets.baskets()