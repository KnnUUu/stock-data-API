import os
import requests
import json
import mysql.connector
from mysql.connector import Error
from datetime import datetime
from financial.financial_API import retrieve_raw_data
from financial.financial_API import process_raw_data
from financial.financial_API import save_to_sql
from financial.financial_API import init_db

def create_db_connection(user, password,host):
    cnx = None
    try:
      cnx = mysql.connector.connect(
        user=user, 
        password=password,
        host=host,
        )
      cursor = cnx.cursor()
      cursor.execute("SHOW DATABASES")
      cursor.execute("CREATE DATABASE DB IF NOT EXISTS")
      print("MySQL Database connection successful")
    except Error as err:
      print(f"Error: '{err}'")

    return cnx



def main():
  cnx = create_db_connection("root","root","localhost")
  
  codes = ['IBM', 'AAPL']
  for code in codes:
    data = retrieve_raw_data(code)
    print(type(data))
    #json_formatted_str = json.dumps(data,indent=2)
    #json.dumps(json_formatted_str,"raw_data.json" )
    
    #print(json_formatted_str)
    #processed_data = process_data(code, data)
    #save_data(processed_data)
    processed_data = process_raw_data(data)
    init_db(cnx)
    #with open(code+".json",'w') as f:
    #  json.dump(processed_data,f)
    
    
    #save_to_sql(processed_data)
    
  cnx.close()
if __name__ == '__main__':
  main()