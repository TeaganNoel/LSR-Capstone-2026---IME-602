#Author: Mason Allen
#Description: Made for connection with Main GUI. Imports a selected csv into a table in MySQL using mysql.connector
#Created Date: 4/5/2026
#Last Updated Date: 4/21/2026

import mysql.connector
from tkinter import filedialog as fd
import pandas as pd
import tkinter as tk
from datetime import datetime
from tkinter import messagebox

from db.queries import get_table_names, get_table_schema

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

def choose_table_window(parent, tables):
    window = tk.Toplevel(parent)
    window.title("Select Table")

    selected_table = tk.StringVar(window)
    selected_table.set(tables[0])

    tk.Label(window, text="Choose table:").pack()
    tk.OptionMenu(window, selected_table, *tables).pack()

    tk.Button(window, text="Confirm", command=window.destroy).pack()

    parent.wait_window(window)
    return selected_table.get()

def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()

def parse_date_time(value):
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection(parent):
   
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(parent=parent, title='Select CSV File', filetypes=[("CSV files","*.csv")])

        #if no file is selected, quit the script
        if not file_path:
            print("No file sected, exiting script...")
            return None
            #exit()

        

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        exit()

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()
   
    #return the long format dataframe
    return file_path


def csv_to_df(file_path):
    df = pd.read_csv(file_path, dtype=str)

    print()
    print("df:",df)
    print()
    #debug
    print("RAW columns:", list(df.columns))
    print()

    return df


def import_data(connection, table_name, df):
    cursor = connection.cursor()

    try: 
        
        # Convert NaN to None for MySQL
        #df = df.where(pd.notnull(df), None)  #<-- DELETE?
        #df = df.where(df=="", None)
        #df = df.replace({pd.NA: None})
        #print(df.columns.tolist())
        #df.columns = df.columns.astype(str)
        #df = df.fillna("")

        #df = df.where(pd.notnull(df), None)

        # Clean null-like values
        df = df.replace("nan", None)
        df = df.replace("", None)

        # Convert NaN properly to None (robust)
        df = df.astype(object).where(pd.notnull(df), None)

        df.columns = df.columns.astype(str)

        # ---- BUILD INSERT STATEMENT ----
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # ---- FAST DATA CONVERSION ----
        data = list(df.itertuples(index=False, name=None))

        # ---- EXECUTE BULK INSERT ----
        cursor.executemany(sql, data)
        connection.commit()

        print("Data imported successfully")
        messagebox.showinfo("Success", "Data imported successfully")

    except mysql.connector.DataError as err:
        print(f"Wrong data type in CSV. Error type {err}")
    
    except mysql.connector.IntegrityError as err:
        print(f"Importing data would affect relational integrity, data import was aborted. Error type {err}")

    except mysql.connector.ProgrammingError as err:
        print(f"An error is present in your SQL syntax, data import was aborted. Error type {err}")

    #may be redundant
    except FileNotFoundError as err:
        print(f"File could not be located. Error type {err}")

    except mysql.connector.Error as err:
        print(f"Something went wrong, data import was aborted. Error type: {err}")

    finally:
        cursor.close()

# 🔹 MAIN ENTRY POINT
def run(parent, connection, database_name):

    # 1️⃣ Get tables
    tables = get_table_names(connection, database_name)

    # 2️⃣ User selects table
    table_name = choose_table_window(parent, tables)
    if not table_name:
        print("No table selected.")
        return None

    # 3️⃣ Get schema
    #schema = get_table_schema(connection, database_name, table_name)

    # 4️⃣ Build fields dynamically
    #fields = build_fields_from_schema(schema) NOT USED

    file_path = open_file_selection(parent)
    if not file_path:
        return

    # 5️⃣ Launch form
    df = csv_to_df(file_path)

    if df is not None:
        import_data(connection, table_name, df)
    
    print("Selected Table:", table_name)
    print(df)



