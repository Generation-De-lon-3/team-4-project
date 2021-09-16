import pandas as pd
from connection import conn


def branches(data):
    
    connection = conn()
    cursor = connection.cursor()
    
    branches = pd.read_sql_query("SELECT branch_name FROM branches;", connection)
    
    branchvalues = []
    
    for each in data:
        if each["branch_name"] not in branches.values and each["branch_name"] not in branchvalues:
            branchvalues.append(each['branch_name'])
        continue
    
    if branchvalues:       
        cursor.execute("INSERT INTO branches (branch_name) VALUES ("+(' ,'.join(branchvalues).join(map(lambda x: "'" + x + "'", branchvalues))+");"))
        connection.commit()
    cursor.close()
    connection.close()
    