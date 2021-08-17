import csv 

cafe_data = []
try:
    with open ('2021-02-23-isle-of-wight.csv', 'r') as file:
        reader = csv.DictReader(file, fieldnames=['timestamp_of_purhcase','store_name','customer_name','basket_items','payment_method','total_price','card_number'])
        next(reader,None)
        i=0
        for line in reader:
            for value in line.values():
                if value == "" or value == "None":
                    i+=1
                    print(line)
                    print(i)  
            # cafe_data.append(line)
except Exception as e:
    print(e)

# print(cafe_data)

