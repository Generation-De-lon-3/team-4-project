import csv 

cafe_data = []

with open ('isle_of_wight', 'r') as file:
    reader = csv.DictReader(file)
    for line in reader:
        cafe_data.append(line)

print(cafe_data)