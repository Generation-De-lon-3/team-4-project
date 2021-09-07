import boto3
import numpy
import pandas as pd

def cafe_dict():
    
    # # Get key and bucket information
    # key = event['Records'][0]['s3']['object']['key']
    # bucket = event['Records'][0]['s3']['bucket']['name']
    
    # # use boto3 library to get object from S3
    # s3_object = s3.get_object(Bucket=bucket, Key=key)
    
    
    # data = s3_object['Body'].read().decode('utf-8')
    # read CSV
    # csv_data = csv.reader(data.splitlines())
    # print(csv_data)
    
    
    s3 = boto3.client('s3')
    s3_object = s3.get_object(Bucket="delon3-team-4-bucket", Key="2021/8/31/birmingham_31-08-2021_09-00-00.csv")
    cafe_data = pd.read_csv(s3_object["Body"], sep=",", names=["order_timestamp", "branch_name", "customer", "basket", "payment_total", "payment_method", "card_type"])

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
        
    
    return cafe_dict