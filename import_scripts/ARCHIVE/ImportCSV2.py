#Author: Mason Allen
#Description: Imports a selected csv into a table in MySQL using mysql.connector
#Created Date: 3/1/2026
#Last Updated Date: 3/1/2026

import csv
import mysql.connector
from tkinter import filedialog as fd
from mysql.connector import Error

import pandas as pd
import tkinter as tk
import warnings
from tkinter import messagebox
from datetime import datetime

server_connect = mysql.connector.connect(user='root',password='Bikegofast!777',host='localhost',database='lsr_testing_database')

def get_table_names(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{database_name}'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    return tables

def get_table_schema(connection, database_name, table_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '{database_name}'
        AND table_name = '{table_name}'
        ORDER BY ordinal_position
    """)
    
    columns = cursor.fetchall()
    cursor.close()

    #DEBUG
    #print("columns:",columns)

    return columns

def mysql_to_python_type(mysql_type):

    type_map = {
        "int": int,
        "bigint": int,
        "smallint": int,
        "float": float,
        "double": float,
        "decimal": float,
        "varchar": str,
        "text": str,
        "date": lambda x: datetime.strptime(x, "%Y-%m-%d").date(),
        "datetime": lambda x: datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
    }

    return type_map.get(mysql_type.lower(), str)

def build_fields_from_schema(schema_rows):
    fields = []

    for column_name, mysql_type in schema_rows:
        fields.append({
            "name": column_name,
            "label": f"{column_name} ({mysql_type})",
            "type": mysql_to_python_type(mysql_type)
        })

    return fields

def choose_table_window(tables):
    

    window = tk.Tk()
    window.title("Select Table")

    selected_table = tk.StringVar(master=window)
    selected_table.set(tables[0])

    tk.Label(window, text="Choose table:").pack()

    dropdown = tk.OptionMenu(window, selected_table, *tables)
    dropdown.pack()

    def confirm():
        window.destroy()

    tk.Button(window, text="Confirm", command=confirm).pack()

    window.mainloop()

    return selected_table.get()

def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()

def parse_date_time(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection():
   
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(title='Select CSV File', filetypes=[("CSV files","*.csv")])
       
        #if no file is selected, quit the script
        if file_path == None:
            print("No file sected, exiting script...")
            exit()


    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        exit()

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()
   
    #return the long format dataframe
    return file_path

def import_dataOLD(csv_file_path):
    
    try: 

        server_connect = mysql.connector.connect(user='root',password='Bikegofast!777',host='localhost',database='lsr_testing_database')
        cursor = server_connect.cursor()
        
        with open(csv_file_path, mode='r') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)  # Skip the header row
            for row in reader:
                cursor.execute("INSERT INTO stocks (TradeDate, SPY, GLD, AMZN, GOOG, KPTI, GILD, MPC) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", row)

        server_connect.commit()

    except mysql.connector.DataError as err:
        print(f"Wrong data type in CSV. Error type {err}")
    
    except mysql.connector.IntegrityError as err:
        print(f"Importing data would affect relational integrity, data import was aborted. Error type {err}")

    except mysql.connector.IntegrityError as err:
        print(f"MySQL version types do not match for a feature, data import was aborted. Error type {err}")

    except mysql.connector.ProgrammingError as err:
        print(f"An error is present in your SQL syntax, data import was aborted. Error type {err}")

    #may be redundant
    except FileNotFoundError as err:
        print(f"File could not be located. Error type {err}")

    except mysql.connector.Error as err:
        print(f"Something went wrong, data import was aborted. Error type: {err}")

    cursor.close()
    server_connect.close()

def csv_to_df(file_path):
    df = pd.read_csv(file_path)

    print()
    print("df:",df)
    print()

    return df


def import_data(connection, table_name, df):
    cursor = connection.cursor()
    #trimmed_table_name = table_name.split('.', 1)[-1]

    try: 
        
        # ---- NA CHECK ----
        #if df.isnull().values.any():
            #warnings.warn("WARNING: DataFrame contains NULL values. They will be inserted as SQL NULL.")

        # Convert NaN to None for MySQL
        df = df.where(pd.notnull(df), None)  #<-- DELETE?
        #df = df.where(df=="", None)
        df = df.replace({pd.NA: None})
        #print(df.columns.tolist())
        df.columns = df.columns.astype(str)
        df = df.fillna("")

        #text_cols = df.select_dtypes(include=['object', 'string']).columns
        #df[text_cols] = df[text_cols].fillna("no data")

        # ---- BUILD INSERT STATEMENT ----
        columns = ', '.join(df.columns)
        print()
        print("COLUMNS:",columns)
        print()
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        

        # ---- FAST DATA CONVERSION ----
        data = list(df.itertuples(index=False, name=None))

        # ---- EXECUTE BULK INSERT ----
        cursor.executemany(sql, data)
        server_connect.commit()

        print("Data imported successfully")

    except mysql.connector.DataError as err:
        print(f"Wrong data type in CSV. Error type {err}")
    
    except mysql.connector.IntegrityError as err:
        print(f"Importing data would affect relational integrity, data import was aborted. Error type {err}")

    except mysql.connector.IntegrityError as err:
        print(f"MySQL version types do not match for a feature, data import was aborted. Error type {err}")

    except mysql.connector.ProgrammingError as err:
        print(f"An error is present in your SQL syntax, data import was aborted. Error type {err}")

    #may be redundant
    except FileNotFoundError as err:
        print(f"File could not be located. Error type {err}")

    except mysql.connector.Error as err:
        print(f"Something went wrong, data import was aborted. Error type: {err}")

    cursor.close()
    server_connect.close()


def run_data_entry(connection, database_name):

    # 1️⃣ Get tables
    tables = get_table_names(connection, database_name)

    # 2️⃣ User selects table
    table_name = choose_table_window(tables)

    if not table_name:
        print("No table selected.")
        return None

    # 3️⃣ Get schema
    schema = get_table_schema(connection, database_name, table_name)

    # 4️⃣ Build fields dynamically
    #DELETE??
    fields = build_fields_from_schema(schema)

    file_path = open_file_selection()

    # 5️⃣ Launch form
    df = csv_to_df(file_path)

    return table_name, df


table_name, df = run_data_entry(server_connect, "lsr_testing_database")

import_data(server_connect, table_name, df)

print("Selected Table:", table_name)
print(df)

#file_path = open_file_selection()
#print("filepath:",file_path)
#import_data(file_path)