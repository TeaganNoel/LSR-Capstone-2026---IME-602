#Author: Mason Allen
#Description: imports a single entity based on a form input
#Created Date: 2/28/2026
#Last Updated Date: 2/28/2026

import pandas as pd
import mysql.connector
import tkinter as tk
from mysql.connector import Error
import warnings
from tkinter import messagebox
from datetime import datetime

#establish connection to local MySQL database
server_connect = mysql.connector.connect(user='root',password='Bikegofast!777',host='localhost',database='lsr_testing_database')

#retrive table names from the database using a MySQL connector (exectes a small SQL query)
def get_table_names(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{database_name}'
    """)
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    #DEBUG
    #print("tables:",tables)

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

def form_query(fields):
    
    entry_window = tk.Tk()
    entry_window.title("Data Entry Form")

    entries = {}

    df = None

    # Create labels + entry widgets dynamically
    for i, field in enumerate(fields):
        tk.Label(entry_window, text=field["label"]).grid(row=i, column=0, sticky=tk.W)
        
        e = tk.Entry(entry_window)
        e.grid(row=i, column=1)
        
        entries[field["name"]] = e

    def get_entered_values():
        
        nonlocal df
        
        data = {}

        for field in fields:
            raw_value = entries[field["name"]].get()

            # ❌ Empty values not allowed
            if raw_value == "":
                messagebox.showerror(
                    "Input Error",
                    f"{field['label']} cannot be empty."
                )
                return  # Stop — keep window open

            # ❌ Wrong datatype
            try:
                value = field["type"](raw_value)
            except ValueError:
                messagebox.showerror(
                    "Input Error",
                    f"{field['label']} must be of type {field['type'].__name__}."
                )
                return  # Stop — keep window open

            data[field["name"]] = [value]

        # ✅ If all fields valid
        df = pd.DataFrame(data)
        #print(df)

        entry_window.destroy()  # Close window only if valid

        return df

    tk.Button(entry_window, text='Quit',
              command=entry_window.destroy).grid(row=len(fields), column=0, pady=4)

    tk.Button(entry_window, text='Import',
              command=get_entered_values).grid(row=len(fields), column=1, pady=4)

    entry_window.mainloop()

    return df

def import_data(connection, table_name, df):
    cursor = connection.cursor()
    #trimmed_table_name = table_name.split('.', 1)[-1]

    try: 
        
        # ---- NA CHECK ----
        if df.isnull().values.any():
            warnings.warn("WARNING: DataFrame contains NULL values. They will be inserted as SQL NULL.")

        # Convert NaN to None for MySQL
        df = df.where(pd.notnull(df), None)

        # ---- BUILD INSERT STATEMENT ----
        columns = ', '.join(df.columns)
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
    fields = build_fields_from_schema(schema)

    # 5️⃣ Launch form
    df = form_query(fields)

    return table_name, df

#MAIN
########################################################

table_name, df = run_data_entry(server_connect, "lsr_testing_database")

import_data(server_connect, table_name, df)

print("Selected Table:", table_name)
print(df)