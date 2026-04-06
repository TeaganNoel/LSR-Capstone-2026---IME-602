#Author: Mason Allen
#Description: Made for connection with Main GUI. Generates a table of ID-Key relations for quick reference from database
#Created Date: 4/4/2026
#Last Updated Date: 4/4/2026

import pandas as pd
import mysql.connector
import tkinter as tk
from tkinter import ttk

# 🔹 DO NOT connect globally if you want control from main GUI
# (optional but recommended)

def choose_table_window(parent, tables):

    window = tk.Toplevel(parent)
    window.title("Select Table")

    selected_table = tk.StringVar(window)
    selected_table.set(tables[0])

    tk.Label(window, text="Choose table:").pack()

    dropdown = tk.OptionMenu(window, selected_table, *tables)
    dropdown.pack()

    def confirm():
        window.destroy()

    tk.Button(window, text="Confirm", command=confirm).pack()

    # Wait until window closes
    parent.wait_window(window)

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
    cursor.execute(f"SELECT `{col_name}` FROM `{table_name}`")
    ID_col = [row[0] for row in cursor.fetchall()]
    print("IDs:",ID_col)
    cursor.close()

    cursor = connection.cursor()
    cursor.execute(f"SELECT name_attr FROM {table_name}")
    name_col = [row[0] for row in cursor.fetchall()]
    cursor.close()

    return ID_col, name_col


def display_table(parent, ID_col, name_col):
    window = tk.Toplevel(parent)
    window.title("ID-Name Table")

    # create table
    table = ttk.Treeview(window, columns=("ID", "Name"), show="headings")

    # column headers
    table.heading("ID", text="ID")
    table.heading("Name", text="Name")

    # column widths
    table.column("ID", width=100)
    table.column("Name", width=200)

    # insert rows
    for i, j in zip(ID_col, name_col):
        table.insert("", tk.END, values=(i, j))

    table.pack(fill="both", expand=True)


# 🔹 Main entry function (THIS is what your button will call)
def run(parent, connection, database_name):

    tables = get_table_names(connection, database_name)

    table_name = choose_table_window(parent, tables)

    if not table_name:
        print("No table selected.")
        return

    ID_col, name_col = get_ID_name_cols(connection, database_name, table_name)

    display_table(parent, ID_col, name_col)