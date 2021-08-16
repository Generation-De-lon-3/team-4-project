import csv 

cafe_data = []

path = r'C:\Users\lovef\Desktop\generation\Group_project\team-4-project\isle_of_wight.csv'
with open (path, 'r') as file:
    reader = csv.DictReader(file)
    for line in reader:
        cafe_data.append(line)

print(cafe_data)