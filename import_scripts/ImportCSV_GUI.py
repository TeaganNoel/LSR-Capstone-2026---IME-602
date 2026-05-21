"""
############################################################################
#Program Filename: ImportCSV_GUI.py
#Author: Mason Allen
#Created Date: 4/5/2026
#Last Updated Date: 5/15/2026
#Description: Imports a CSV into a db table based on a user file
#             selection. Made for connection with Import Main GUI.
############################################################################
"""

#import necessary libraries
import mysql.connector
from tkinter import filedialog as fd
import pandas as pd
import tkinter as tk
import ttkbootstrap as ttk
from datetime import datetime
from tkinter import messagebox

#import used scripts
from db.queries import get_table_names

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

def choose_table_windowOLD(parent, tables):
    
    if not tables:
        messagebox.showerror(
            "Error",
            "No valid tables found.",
            parent=parent
        )
        return None
    
    window = tk.Toplevel(parent)
    window.title("Select Table")

    selected_table = tk.StringVar(window)
    selected_table.set(tables[0])

    tk.Label(window, text="Choose table:").pack()
    tk.OptionMenu(window, selected_table, *tables).pack()

    tk.Button(window, text="Confirm", command=window.destroy).pack()

    parent.wait_window(window)
    return selected_table.get()

def choose_table_windowOLD2(parent, tables):

    window = tk.Toplevel(parent)

    window.title("Select Table")

    # Strong modal behavior
    window.transient(parent)
    window.grab_set()
    window.focus_force()
    window.resizable(False, False)

    selected_table = tk.StringVar(window)
    selected_table.set(tables[0])

    tk.Label(window, text="Choose table:").pack()

    dropdown = tk.OptionMenu(window, selected_table, *tables)
    dropdown.pack()

    def confirm():
        window.destroy()

    def cancel():
        selected_table.set("")
        window.destroy()

    # Handle clicking X button
    window.protocol(
        "WM_DELETE_WINDOW",
        cancel
    )

    tk.Button(
        window,
        text="Confirm",
        command=confirm
    ).pack()

    parent.wait_window(window)

    return selected_table.get()

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

def parse_date(value):
############################################################################
#Function Name: parse_date
#Description: turn a data value into a datetime value
#Parameters: value --> string date value
#Return Values: return datetime.strptime(value, "%Y-%m-%d").date() --> formatted date
############################################################################

    return datetime.strptime(value, "%Y-%m-%d").date()

def parse_date_time(value):
############################################################################
#Function Name: parse_date_time
#Description: turn a data value into a datetime value
#Parameters: value --> string date time value
#Return Values: return datetime.strptime(value, "%Y-%m-%d %H:%M:%S") --> formatted datetime
############################################################################ 
    
    return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

def open_file_selection(parent):
############################################################################
#Function Name: open_file_selection
#Description: prompts user with a file selection window to choose a CSV. 
#Parameters: parent --> parent ttk window
#Return Values: file_path --> file path of selected csv
############################################################################

    #try opening file and get file name
    try:
        file_path = fd.askopenfilename(parent=parent, title='Select CSV File', filetypes=[("CSV files","*.csv")])

        #if no file is selected, quit the script
        if not file_path:
            print("No file sected, exiting script...")
            return None
       
    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        return None

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
   
    return file_path

def csv_to_df(file_path):
############################################################################
#Function Name: csv_to_df
#Description: reads and converts CSV to dataframe
#Parameters: file_path --> file path of selected csv
#Return Values: df --> dataframe of csv data
############################################################################
    
    df = pd.read_csv(file_path, dtype=str)

    print("df:",df)
    print("RAW columns:", list(df.columns))

    return df

def import_data(connection, table_name, df):
############################################################################
#Function Name: import_data
#Description: Connects to the database using a MySQL Connector cursor and 
#             imports a new row of data into the selected table. Uses SQL
#Parameters: connection --> MySQL db connection for queries and imports
#            table_name --> name of table to import into
#            df --> data frame of values to be imported to the db table
#Return Values: success --> T/F import status
#               error_msg --> import error text if import fails
############################################################################

    cursor = connection.cursor()

    try: 

        # Clean null-like values
        df = df.replace("nan", None)
        df = df.replace("", None)

        # Convert NaN properly to None (robust)
        df = df.astype(object).where(pd.notnull(df), None)

        df.columns = df.columns.astype(str)

        # ---- BUILD INSERT STATEMENT ----
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))

        sql = f"""
            INSERT INTO {table_name}
            ({columns})
            VALUES ({placeholders})
        """
        
        # ---- FAST DATA CONVERSION ----
        data = list(df.itertuples(index=False, name=None))

        # ---- EXECUTE BULK INSERT ----
        cursor.executemany(sql, data)
        connection.commit()

        print("Data imported successfully")

        messagebox.showinfo(
            "Success",
            "Data imported successfully"
        )

        return True, None

    except mysql.connector.DataError as err:

        error_msg = (
            f"Wrong data type in CSV.\n\nError:\n{err}"
        )

        print(error_msg)

        return False, error_msg
    
    except mysql.connector.IntegrityError as err:

        error_msg = (
            "Importing data would affect relational integrity, "
            f"data import was aborted.\n\nError:\n{err}"
        )

        print(error_msg)

        return False, error_msg

    except mysql.connector.ProgrammingError as err:

        error_msg = (
            "An error is present in your SQL syntax, "
            f"data import was aborted.\n\nError:\n{err}"
        )

        print(error_msg)

        return False, error_msg

    except FileNotFoundError as err:

        error_msg = (
            f"File could not be located.\n\nError:\n{err}"
        )

        print(error_msg)

        return False, error_msg

    except mysql.connector.Error as err:

        error_msg = (
            "Something went wrong, data import was aborted.\n\n"
            f"Error:\n{err}"
        )

        print(error_msg)

        return False, error_msg

    finally:
        cursor.close()

#IMPORT MAIN GUI ENTRY
def run(parent, connection, database_name):
############################################################################
#Function Name: run
#Description: main function to run all other functions in script. This is
#             the entry point from the Import Main GUI, and allows user to
#             select a table to import into.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: N/A
############################################################################

    tables = get_table_names(connection, database_name)

    table_name = choose_table_window(parent, tables)

    if not table_name:
        print("No table selected.")
        return None

    file_path = open_file_selection(parent)

    if not file_path:
        return

    df = csv_to_df(file_path)

    if df is not None:

        success, error_msg = import_data(
            connection,
            table_name,
            df
        )

        if not success:

            messagebox.showerror(
                "Import Failed",
                error_msg,
                parent=parent
            )

            return False, table_name
    
    print("Selected Table:", table_name)
    print(df)
