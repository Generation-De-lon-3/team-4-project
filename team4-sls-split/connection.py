import boto3
import psycopg2


def conn():
    
    client = boto3.client('redshift', region_name='eu-west-1')

    REDSHIFT_USER = "awsuser"
    REDSHIFT_CLUSTER = "redshiftcluster-jlqz8zhcuit6"
    REDSHIFT_HOST = "redshiftcluster-jlqz8zhcuit6.cc3hslvy2bfm.eu-west-1.redshift.amazonaws.com"
    REDSHIFT_DATABASE = "team-4-db"
    
    credentials = client.get_cluster_credentials(
        DbUser=REDSHIFT_USER,
        DbName=REDSHIFT_DATABASE,
        ClusterIdentifier=REDSHIFT_CLUSTER,
        DurationSeconds=3600)
    
    connection = psycopg2.connect(
        user=credentials['DbUser'], 
        password=credentials['DbPassword'],
        host=REDSHIFT_HOST,
        database=REDSHIFT_DATABASE,   
        port=5439)

    return connection
    
    
def initdb():
    
    connection = conn()
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT IDENTITY (1,1) NOT NULL,
            product_name varchar(100) NOT NULL,
            product_size varchar(10),
            product_price float NOT NULL,
        UNIQUE (product_name, product_size, product_price),
        PRIMARY KEY (product_id)
        );

        CREATE TABLE IF NOT EXISTS branches (
        branch_id INT IDENTITY (1,1) NOT NULL,
        branch_name varchar(22) NOT NULL,
        UNIQUE (branch_name),
        PRIMARY KEY (branch_id)
        );

        CREATE TABLE IF NOT EXISTS orders (
            order_id INT IDENTITY (1,1) NOT NULL,
            order_timestamp timestamp NOT NULL,
            branch_id int NOT NULL,
        FOREIGN KEY (branch_id) REFERENCES branches (branch_id),
        UNIQUE (order_timestamp, branch_id),
        PRIMARY KEY (order_id)
        );

        CREATE TABLE IF NOT EXISTS baskets (
            order_id int NOT NULL,
            product_id int NOT NULL,
            product_quantity int NOT NULL,
        FOREIGN KEY (product_id) REFERENCES products (product_id),
        FOREIGN KEY (order_id) REFERENCES orders (order_id),
        UNIQUE (order_id, product_id),
        PRIMARY KEY (order_id, product_id)
        );

        CREATE TABLE IF NOT EXISTS payments (
            payment_id int NOT NULL,
            payment_method varchar(10) NOT NULL,
            card_type varchar(22),
            payment_total float NOT NULL,
        FOREIGN KEY (payment_id) REFERENCES orders (order_id),
        UNIQUE (payment_id),
        PRIMARY KEY (payment_id)
        );
    """)
    
    connection.commit()
    cursor.close()
    connection.close()
