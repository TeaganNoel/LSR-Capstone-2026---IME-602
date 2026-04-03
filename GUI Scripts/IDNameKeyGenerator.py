#Author: Mason Allen
#Description: Generates a table of ID-Key relations for quick reference from database
#Created Date: 3/7/2026
#Last Updated Date: 3/7/2026

import pandas as pd
import mysql.connector
import tkinter as tk
from mysql.connector import Error
import warnings
from tkinter import messagebox
from tkinter import ttk
from datetime import datetime

#establish connection to local MySQL database
server_connect = mysql.connector.connect(user='root',password='Bikegofast!777',host='localhost',database='lsr_testing_database')

#displays a table selection window for the user to choose a db table
def choose_table_window(tables):

    #start tkinter popup window
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



def get_table_names(connection, database_name):
    cursor = connection.cursor()
    cursor.execute(f"""
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = '{database_name}' AND column_name = 'name_attr'
        AND table_name NOT LIKE '%rel_%'
    """)

    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    
    #DEBUG
    #print("tables:",tables)

    return tables

def get_ID_name_cols(connection, database_name, table_name):
    cursor = connection.cursor()
    
        # Step 1: get first column name
    query = """
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema = %s
        AND table_name = %s
        AND ORDINAL_POSITION = 1
    """
    cursor.execute(query, (database_name, table_name))
    col_name = cursor.fetchone()[0]

    # Step 2: get values from that column
    query2 = f"SELECT `{col_name}` FROM `{table_name}`"
    cursor.execute(query2)

    ID_col = [row[0] for row in cursor.fetchall()]
    print("IDs:",ID_col)
    cursor.close()

    cursor = connection.cursor()
    query3 = f"SELECT name_attr FROM {table_name}"
    cursor.execute(query3)
    
#        FROM {database_name}.{table_name}

    name_col = [row[0] for row in cursor.fetchall()]
    print("Names:",name_col)
    cursor.close()
    #DEBUG
    #print("columns:",columns)

    return ID_col, name_col 

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

  
def display_table(ID_col, name_col):
    root = tk.Tk()
    root.title("Two Column Table")

    #data = [ID_col,name_col]

    # create table
    table = ttk.Treeview(root, columns=("ID", "Name"), show="headings")

    # column headers
    table.heading("ID", text="ID")
    table.heading("Name", text="Name")

    # column widths
    table.column("ID", width=100)
    table.column("Name", width=200)

    # insert rows
    #for row in data:
    for i, j in zip(ID_col, name_col):
        table.insert("", tk.END, values=(i, j))

    table.pack(fill="both", expand=True)

    root.mainloop()

def main(connection, database_name):
    
    # 1️⃣ Get tables
    tables = get_table_names(connection, database_name)
    
    # 2️⃣ User selects table
    table_name = choose_table_window(tables)
    
    if not table_name:
        print("No table selected.")
        return None
    
    #schema = get_table_schema(connection, database_name, table_name)



    ID_col, name_col = get_ID_name_cols(connection, database_name, table_name)
    
    display_table(ID_col,name_col)

    
main(server_connect,"lsr_testing_database")

