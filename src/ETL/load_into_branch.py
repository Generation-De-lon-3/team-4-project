import psycopg2
import pandas 
import clean_data as app


#establish database connection
connection = psycopg2.connect(
    host = "localhost",
    user = "root",
    password = "password",
    database = "cafe",
)

#DB cursor
cursor = connection.cursor()


#branch table
cursor.executemany("INSERT INTO branches(branch_name) VALUES (%(branch)s)",app.branch_data)
connection.commit()
print(cursor.rowcount,"record(s) inserted.")


cursor.close()

#close the connection
connection.close()