import app
import pandas as pd


def branches():

    connection = app.connection()
    cursor = connection.cursor()

    branches = pd.read_sql_query("SELECT branch_name FROM branches", connection)

    values = []
    
    for each in app.cafe_dict():
        if each["branch"] not in branches.branch_name.values:
            values.append(each['branch'])  
    if values:       
        cursor.execute(f"INSERT INTO branches (branch_name) VALUES {' ,'.join(values)};")
        connection.commit()
    cursor.close()
    connection.close()


branches()
