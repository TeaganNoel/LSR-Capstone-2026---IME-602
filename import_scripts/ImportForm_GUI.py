"""
############################################################################
#Program Filename: ImportForm_GUI.py
#Author: Mason Allen
#Created Date: 4/4/2026
#Last Updated Date: 5/4/2026
#Description: Imports a single entity (row) into a db table based on a user 
#             form input. Made for connection with Import Main GUI.
############################################################################
"""

#import necessary libraries
import pandas as pd
import tkinter as tk
from tkinter import messagebox
import ttkbootstrap as ttk
from datetime import datetime
import warnings

from db.queries import get_table_names, get_table_schema


def mysql_to_python_type(mysql_type):
############################################################################
#Function Name: mysql_to_python_type
#Description: Maps MySQL data types to Python datatypes for error checking
#             and input conversion.
#Parameters: mysql_type --> data type map of table in MySQL
#Return Values: type_map.get(mysql_type.lower(), str) --> returns conversion
#               functions
############################################################################

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
############################################################################
#Function Name: build_fields_from_schema
#Description: Compiles features of the table for creating a user input form,
#             including the table name, the label for the form row, and the 
#             data type for each feature.
#Parameters: schema_rows --> table schema from db, used to get table field info
#Return Values: fields --> returns the name, label, and type for each field
#               in the table for the input form
############################################################################
    
    fields = []

    for column_name, mysql_type in schema_rows:
        fields.append({
            "name": column_name,
            "label": f"{column_name} ({mysql_type})",
            "type": mysql_to_python_type(mysql_type)
        })

    return fields

def choose_table_window(parent, tables):
############################################################################
#Function Name: choose_table_window
#Description: Prompts user to select a table to import data into
#Parameters: parent --> parent ttk window
#            tables --> list of tables in the database
#Return Values: result['table'] --> user selected table name
############################################################################

    result = {"table": None}

    window = tk.Toplevel(parent)
    window.title("Select Table")

    window.transient(parent)
    window.grab_set()
    window.resizable(False, False)

    frame = tk.Frame(window, padx=15, pady=15)
    frame.pack()

    tk.Label(frame, text="Choose table:", font=("Segoe UI", 11)).pack(pady=(0,10))

    selected_table = tk.StringVar(value=tables[0])

    dropdown = ttk.Combobox(
        frame,
        textvariable=selected_table,
        values=tables,
        state="readonly",
        width=30
    )
    dropdown.pack(pady=5)
    dropdown.focus_set()

    def confirm():
        result["table"] = selected_table.get()
        window.destroy()

    def cancel():
        result["table"] = None
        window.destroy()

    window.protocol("WM_DELETE_WINDOW", cancel)

    btn_frame = tk.Frame(frame)
    btn_frame.pack(pady=10)

    tk.Button(btn_frame, text="Confirm", width=10, command=confirm).pack(side="left", padx=5)
    tk.Button(btn_frame, text="Cancel", width=10, command=cancel).pack(side="left", padx=5)

    parent.wait_window(window)
    return result["table"]

def form_query(parent, fields):
############################################################################
#Function Name: form_query
#Description: Displays a form input window that matches the fields in the 
#             selected db table. Checks input to ensure that user does not
#             enter blank values or the wrong data types
#Parameters: parent --> parent ttk window
#            fields --> field information for each row of input form
#Return Values: df --> data frame of values to be imported to the db table
############################################################################
    
    entry_window = tk.Toplevel(parent)
    entry_window.title("Data Entry Form")
    entry_window.transient(parent)
    entry_window.grab_set()

    entries = {}
    df = None

    frame = tk.Frame(entry_window, padx=15, pady=15)
    frame.pack()

    for i, field in enumerate(fields):
        tk.Label(frame, text=field["label"]).grid(row=i, column=0, sticky="w", pady=3)
        e = tk.Entry(frame)
        e.grid(row=i, column=1, pady=3)
        entries[field["name"]] = e

    def submit():
        nonlocal df
        data = {}

        for field in fields:
            raw = entries[field["name"]].get().strip()

            if raw == "":
                messagebox.showerror("Error", f"{field['label']} cannot be empty.")
                return

            try:
                value = field["type"](raw)

            except Exception:
                messagebox.showerror("Error", f"{field['label']} invalid type.")
                return

            data[field["name"]] = [value]

        df = pd.DataFrame(data)
        entry_window.destroy()

    entry_window.protocol("WM_DELETE_WINDOW", entry_window.destroy)

    btn_row = len(fields)

    tk.Button(frame, text="Import", command=submit)\
        .grid(row=btn_row, column=1, pady=10)

    tk.Button(frame, text="Cancel", command=entry_window.destroy)\
        .grid(row=btn_row, column=0, pady=10)

    parent.wait_window(entry_window)
    return df

def import_data(connection, table_name, df):
############################################################################
#Function Name: import_data
#Description: Connects to the database using a MySQL Connector cursor and 
#             imports a new row of data into the selected table. Uses SQL
#Parameters: connection --> MySQL db connection for queries and imports
#            table_name --> name of table to import into
#Return Values: df --> data frame of values to be imported to the db table
############################################################################

    cursor = connection.cursor()

    try:
        if df.isnull().values.any():
            warnings.warn("NULL values will be inserted.")

        df = df.where(pd.notnull(df), None)

        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        data = list(df.itertuples(index=False, name=None))

        cursor.executemany(sql, data)
        connection.commit()

        print("Data imported successfully")
        
        return True

    except Exception as err:
        print(f"Import failed: {err}")

        return False
    
    finally:
        cursor.close()

#IMPORT MAIN ENTRY
def run(parent, connection, database_name):
############################################################################
#Function Name: run
#Description: main function to run all other functions in script. This is
#             the entry point from the Import Main GUI, and allows user to
#             select a table to import into.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: returns a success T/F and the table name into which data was
#               imported
############################################################################

    tables = get_table_names(connection, database_name)
    if not tables:
        messagebox.showerror("Error", "No tables found.", parent=parent)
        return False
    
    table_name = choose_table_window(parent, tables)
    if not table_name:
        return False

    schema = get_table_schema(connection, database_name, table_name)

    fields = build_fields_from_schema(schema)

    df = form_query(parent, fields)

    if df is not None:
        return import_data(connection, table_name, df), table_name
    
    else:
        return False

#RUN ALL ENTRY    
def run_auto(parent, connection, database_name, table):
############################################################################
#Function Name: run_auto
#Description: main function to run all other functions in script. This is
#             the entry point from the RUN ALL script. Has a preselected 
#             import table.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#            table --> name of db table
#Return Values: returns a success T/F and the table name into which data was
#               imported
############################################################################

    table_name = table
    if not table_name:
        return False

    schema = get_table_schema(connection, database_name, table_name)

    fields = build_fields_from_schema(schema)

    df = form_query(parent, fields)

    if df is not None:
        return import_data(connection, table_name, df)
