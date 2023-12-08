import json
import pyodbc
import datetime
import numpy as np
import pandas as pd
import yfinance as yfin
from pandas_datareader import data as pdr
yfin.pdr_override()
from password import password

connection_string = f"Driver={{ODBC Driver 18 for SQL Server}};Server=tcp:rriverbay55.database.windows.net,1433;Database=developdatabase;Uid=rriverbay55;Pwd={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
def connect_to_database(connection_string=connection_string):
    try:
        # Establish the database connection
        conn = pyodbc.connect(connection_string)
        # return the connection
        return conn
    except Exception as e:
        print(f"Error: {e}")
        return None

def close_connection(conn):
    conn.close()
    print('Connection closed!')

# a funtion to convert pandas data type to SQL data type
def dtype_to_sql(type):
    result = "VARCHAR(50)"
    if type == np.dtype('float64'):
        return "FLOAT"
    if type == np.dtype('int64'):
        return "INT"
    if type == np.dtype('bool'):
        return "BIT"
    return "VARCHAR(50)"

# a function to return the string to create table
def create_table_sql(df, table_name):
    columns = df.columns.to_numpy()
    dtypes = df.dtypes.to_numpy()
    sql =   f"CREATE TABLE {table_name} ( "
    sql +=  f"Date DATE PRIMARY KEY,"
    for name, type in zip(columns, dtypes):
        sql += f' [{name.replace(" ", "_")}] {dtype_to_sql(type)} NOT NULL,'
    # remove last ","
    sql = sql [:-1]
    sql +=  " );"
    return sql

def create_table(conn, table_name, df):
    # create a cursor
    cursor = conn.cursor()
    print('Cursor created.')

    # drop table if it exists
    check_table_sql = f'''
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        SELECT 1 AS TableExists
    ELSE
        SELECT 0 AS TableExists
    '''
    cursor.execute(check_table_sql)
    print('Checking if table exists.')
    
    # get checking result
    table_exists = cursor.fetchone()[0]
    # drop an existing table
    if table_exists:
        print('Table exists, dropping existing table...')
        cursor.execute(f'DROP TABLE {table_name}')

    sql_create_table = create_table_sql(df, table_name)
    print('Creating table...')

    cursor.execute(sql_create_table)
    cursor.commit()
    print(f'Cursor committed.')
    cursor.close()
    print(f'Cursor closed.')

def insert_into_table(conn, table_name, df):
    # create a cursor
    cursor = conn.cursor()
    print('Cursor created.')

    sql_insert_table = f"INSERT INTO {table_name} VALUES ({'?,' * (df.shape[1] - 1) + '?'}) "

    print(f'executing: {sql_insert_table} from dataframe...')

    # Execute the batch insert using executemany
    cursor.executemany(sql_insert_table, df.values.tolist())

    # cursor.execute(insert_table_command)
    cursor.commit()
    print(f'Cursor committed.')

    cursor.close()
    print(f'Cursor closed.')


def convert_str_to_date(dt_str):
    return [int(x) for x in dt_str.split("-")]


print('Connecting to database... ')
#make connection to database
db = connect_to_database()
print('Successfully connected with database.')

# read json file to get symbols, start and end
file_path = './config.json'
print(f'Reading {file_path}...')

try:    
    with open(file_path, 'r') as file:
        data    = json.load(file)
        symbols = data.get("symbols", ['AAPL'])
        print(symbols)
        start   = data.get("start", "2022-01-01")
        print(start)
        end     = data.get("end", "2022-12-31")
        print(end)
except FileNotFoundError:
    print(f"The file '{file_path}' does not exist.")
    exit(1)
except json.JSONDecodeError as e:
    print(f"Error decoding JSON: {e}")
    exit(1)
except Exception as e:
    print(f"An unexpected error occurred: {e}")
    exit(1)


# get start time and end time
start_date = datetime.datetime(*convert_str_to_date(start))
end_date = datetime.datetime(*convert_str_to_date(end))

for symbol in symbols:
    print(f'Now fetching {symbol} data from Yahoo...')
    # get data from Yahoo finance
    data = pdr.get_data_yahoo(symbol, start = start_date, end = end_date)
    print(f'{symbol} data fetched successfully.')
    # create table in database
    create_table(db, symbol, data)
    print(f'Table {symbol} created.')
    # add index value into dataframe
    data_with_index = data.reset_index()
    # insert data into table just created
    insert_into_table(db, symbol, data_with_index)
    print(f'Table {symbol} value updated from dataframe.')
# close connection to dababase
close_connection(db)  