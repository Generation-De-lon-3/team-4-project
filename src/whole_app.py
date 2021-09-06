import boto3
import pandas as pd
import psycopg2
import json


def handle(event, context):
    # Get key and bucket information
    key = event['Records'][0]['s3']['object']['key']
    bucket = event['Records'][0]['s3']['bucket']['name']
    
    # use boto3 library to get object from S3
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket=bucket, Key=key)
    # data = s3_object['Body'].read().decode('utf-8')
    
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    df = pd.read_csv(s3_object["Body"], sep=",", names=["order_timestamp", "branch", "customer", "basket", "payment_total", "payment_method", "card_type"])

    del df["customer"] 

    cafe_dict = df.to_dict('records')

    basket_fields = ['product_size', 'product_name', 'product_price']

    for item in cafe_dict:
        item['basket'] = item['basket'].split(',')
        item['basket'] = [",".join(item['basket'][i:i+3]) for i in range(0, len(item['basket']), 3)]
            
        for index, each in enumerate(item['basket']):
            item['basket'][index] = each.split(',')

        for index, items in enumerate(item['basket']):  
            item['basket'][index] = dict(zip(basket_fields, items))

        item['card_type'] = "".join(i for i in item['card_type'] if i.isalpha())
        
        item["card_type"] = item["card_type"].replace("None", "")
        
    # ---------------------------------------------------------------------------------------------

    client = boto3.client('redshift', region_name='eu-west-1')

    REDSHIFT_USER = "awsuser"
    REDSHIFT_CLUSTER = "redshiftcluster-jlqz8zhcuit6"
    REDSHIFT_DATABASE = "team-4-db"
    REDSHIFT_HOST = "redshiftcluster-jlqz8zhcuit6.cc3hslvy2bfm.eu-west-1.redshift.amazonaws.com",
    
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
    
    # ---------------------------------------------------------------------------------------------
    
    branchval = []
    branches = pd.read_sql_query("SELECT * FROM branches", connection)

    for each in cafe_dict:
        if each["branch"] not in branches.values:
            continue
        branchval.append(each["branch"])

    cursor.execute(f"INSERT INTO branches (branch_name) VALUES ('{' ,'.join(branchval)}');")
    
    # ---------------------------------------------------------------------------------------------
    
    baskets = []

    for item in cafe_dict:
        for index, each in enumerate(item['basket']):
            baskets.append(item['basket'][index])

    baskets2 = [dict(tupleized) for tupleized in set(tuple(item.items()) for item in baskets)]
    baskets3 = pd.DataFrame(baskets2)
    
    val = []
    
    products = pd.read_sql_query("SELECT product_size, product_name, product_price FROM products", connection)
    
    # print(baskets3)
    products['product_price'] = products['product_price'].apply(lambda x: "{:.2f}".format(x))
 
    # print(baskets3)
    # print(products)
 
    final = products.merge(baskets3, how='outer', indicator=True).loc[lambda x: x['_merge'] == 'right_only']
    # print(final)
    
    final = final.to_dict('records')
    
    for item in final:
        val.append(f"('{item['product_name']}', '{item['product_size']}', {item['product_price']})")
        
        # print(val)

    for each in val:
        cursor.execute(f"INSERT INTO products (product_name, product_size, product_price) VALUES {each};")
        connection.commit()

    # ---------------------------------------------------------------------------------------------
    
    cursor.close()
    connection.commit()
    connection.close()
