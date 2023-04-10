import os
import requests
import json
from datetime import datetime

API_KEY = os.environ.get("STOCK_DATA_API_KEY")
API_URL = os.environ.get("STOCK_DATA_API_URL")
DAY_LIMIT = int(os.environ.get("DAY_LIMIT"))

def retrieve_raw_data(symbol):
  """
  retrieve financial data from external API
  """
  data = {}
  params = {
    "function": "TIME_SERIES_DAILY_ADJUSTED",
    "symbol": symbol,
    "apikey": API_KEY
  }
  response = requests.get(API_URL, params)
  
  success_code = 200
  if response.status_code == success_code:
    data = response.json()
  
  return data

def process_raw_data(data):
  """
  Processes the raw data by applying filters
  """
  meta = data.get("Meta Data", {})
  symbol = meta.get("2. Symbol", {})
  series = data.get("Time Series (Daily)", {})
  processed_data = []
  
  for date_str, values in series.items():
    date = datetime.strptime(date_str, "%Y-%m-%d")
    # only Retrieve data from most recently two weeks.
    if (datetime.today() - date).days > DAY_LIMIT:
      continue
    
    open_price = values.get("1. open")
    close_price = values.get("4. close")
    volume = values.get("6. volume")
    
    processed_data.append({
        "symbol":symbol,
        "date":date.isoformat(),
        "open_price":open_price,
        "close_price":close_price,
        "volume":volume
        })
  
  return processed_data

def init_db(connection):
  fd = open('schema.sql', 'r')
  query = fd.read() 
  fd.close()
  
  cursor = connection.cursor(buffered=True)
  #try:
  create_teacher_table = """
  USE db;
CREATE TABLE teacher (
  teacher_id INT PRIMARY KEY,
  first_name VARCHAR(40) NOT NULL,
  last_name VARCHAR(40) NOT NULL,
  language_1 VARCHAR(3) NOT NULL,
  language_2 VARCHAR(3),
  dob DATE,
  tax_id INT UNIQUE,
  phone_no VARCHAR(20)
  );
 """
  
  cursor.execute(create_teacher_table)
  connection.commit()
  print("Database created successfully")
  #except:
  #  print("Database created failed")

  
def save_to_sql(data):
  return