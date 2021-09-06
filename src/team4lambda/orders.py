import pandas as pd
import app

def orders():
    connection = app.connection()
    cursor = connection.cursor()

    branches = pd.read_sql_query("SELECT * FROM branches", connection)
    orders = pd.read_sql_query("SELECT order_timestamp, branch_id FROM orders", connection)

    orders["order_timestamp"] = orders["order_timestamp"].astype(str)

    values = []
            
    for each in app.cafe_dict():
        for branch in branches.iterrows():
            if branch[1]["branch_name"] == each['branch']:
                branchid = branch[1]["branch_id"]
                if str(each["order_timestamp"]) not in orders.values:
                    values.append(f"('{each['order_timestamp']}', '{branchid}')")
                

    print(f"INSERT INTO orders (order_timestamp, branch_id) VALUES {' ,'.join(values)};")
    if values:
        cursor.execute(f"INSERT INTO orders (order_timestamp, branch_id) VALUES {' ,'.join(values)};")
        connection.commit()
    cursor.close()
    connection.close()
    

# orders()