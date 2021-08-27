import psycopg2
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
cursor.executemany("INSERT INTO branch(branch) VALUES (%(branch)s)",app.cafe_data)
connection.commit()
print(cursor.rowcount,"record(s) inserted.")


cursor.close()

#close the connection
connection.close()