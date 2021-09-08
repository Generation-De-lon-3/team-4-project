import conn
# import etl
import pandas as pd


def branches(data):
    
    connection = conn.connection()
    cursor = connection.cursor()

    branches = pd.read_sql_query("SELECT branch_name FROM branches;", connection)

    values = []
    
    for each in data:
        if each["branch_name"] not in branches.values and each["branch_name"] not in values:
            values.append(each['branch_name'])
        continue
    

    if values:       
        cursor.execute("INSERT INTO branches (branch_name) VALUES ("+(' ,'.join(values).join(map(lambda x: "'" + x + "'", values))+");"))
        connection.commit()
    cursor.close()
    connection.close()
