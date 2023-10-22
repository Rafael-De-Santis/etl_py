from sqlalchemy import create_engine ##to inRWExr with postgre sql
import pyodbc ## to query the sql server
import pandas as pd ## to data extra and data load
import os ##  
import requests
import mysql.connector
from mysql.connector import Error



pwd = "demopass"
uid = "etl"
server = "localhost"

## extract data from MySQL
def extract():
    try:
        connection = mysql.connector.connect(host='localhost', 
                                            database='classicmodels',
                                            user=uid,
                                            password=pwd)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to the database: ", record)

            mycursor =  connection.cursor()

            mycursor.execute("""SELECT * from customers""")

            myresult = mycursor.fetchall()

            
            df = pd.read_sql_query(f'select * FROM customers', connection)
            load(df, myresult)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

## load data to postgres
def load(df, tbl):
    try:
        rows_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:5432/Adventure')
        print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {tbl}')
        #save df to postgres
        df.to_sql('fstg_{tbl}', engine, if_exists='replace', index=False)
        rows_imported += len(df)
        #add elapser time to final print out
        print("Data imported sucessful")
    except Error as e:
        print("Data load error: " + str(e))

try:
    #call extract function
    extract()
except Exception as e:
    print("Error while extracting data: " + str(e))