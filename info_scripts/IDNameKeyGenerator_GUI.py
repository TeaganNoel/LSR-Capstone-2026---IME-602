"""
############################################################################
#Program Filename: IDNameKeyGenerator_GUI.py
#Author: Mason Allen
#Created Date: 4/4/2026
#Last Updated Date: 4/29/2026
#Description: Made for connection with Main GUI. Generates a table of ID-Key 
#             relations for quick reference from database
############################################################################
"""

#import necessary libraries
import pandas as pd
import tkinter as tk
from tkinter import ttk

def choose_table_window(parent, tables):
############################################################################
#Function Name: choose_table_window
#Description: creates window which allows user to chose what table to inspect
#Parameters: parent --> parent ttk window
#            tables --> list of db tables
#Return Values: selected_table --> user selected table name
############################################################################
    
    window = tk.Toplevel(parent)
    window.title("Select Table")

    # --- Match modern modal style ---
    window.transient(parent)
    window.grab_set()
    window.resizable(False, False)

    selected_table = tk.StringVar(value=tables[0])

    # --- Layout frame ---
    frame = ttk.Frame(window, padding=15)
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text="Choose table:").grid(row=0, column=0, columnspan=2, pady=(0, 10))

    dropdown = ttk.Combobox(
        frame,
        textvariable=selected_table,
        values=tables,
        state="readonly",
        width=30
    )
    dropdown.grid(row=1, column=0, columnspan=2, pady=(0, 10))
    dropdown.focus_set()

    def confirm():
        window.destroy()

    def cancel():
        selected_table.set("")  # return empty
        window.destroy()

    # if user clicks X
    window.protocol(
        "WM_DELETE_WINDOW",
        cancel
    )

    ttk.Button(frame, text="Confirm", command=confirm).grid(row=2, column=0, pady=5, padx=5)
    ttk.Button(frame, text="Cancel", command=cancel).grid(row=2, column=1, pady=5, padx=5)

    parent.wait_window(window)

    return selected_table.get()

def get_table_names(connection, database_name):
############################################################################
#Function Name: get_table_names
#Description: uses MySQL connector to query and return all table names with 
#             an ID and NAME column
#Parameters: connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: tables --> array of table names from db
############################################################################

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
############################################################################
#Function Name: get_ID_name_cols
#Description: uses MySQL connector to query and return IDs and NAMEs in a  
#             specified table
#Parameters: connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#            table_name --> table name from db
#Return Values: ID_col --> list of IDs from the table
#               name_col --> list of matching names from the table
############################################################################

    cursor = connection.cursor()

    try:
        # Get first (ID) column name
        query = """
            SELECT COLUMN_NAME
            FROM information_schema.columns
            WHERE table_schema=%s
            AND table_name=%s
            AND ORDINAL_POSITION=1
        """

        cursor.execute(
            query,
            (database_name, table_name)
        )

        col_name = cursor.fetchone()[0]


        # Pull IDs and names together
        query = f"""
            SELECT `{col_name}`, name_attr
            FROM `{table_name}`
        """

        cursor.execute(query)

        rows = cursor.fetchall()

        ID_col = [r[0] for r in rows]
        name_col = [r[1] for r in rows]

        print("IDs:", ID_col)

        return ID_col, name_col

    finally:
        cursor.close()

def display_table(parent, ID_col, name_col):
############################################################################
#Function Name: display_table
#Description: display a window that shows all IDs and NAMEs in the table
#Parameters: parent --> parent ttk window
#            ID_col --> list of IDs from the table
#            name_col --> list of matching names from the table
#Return Values: none
############################################################################

    window = tk.Toplevel(parent)
    window.title("ID-Name Table")

    # --- Modal-like feel (optional but consistent) ---
    window.transient(parent)
    window.resizable(True, True)

    frame = ttk.Frame(window, padding=10)
    frame.pack(fill="both", expand=True)

    # --- Treeview ---
    table = ttk.Treeview(
        frame,
        columns=("ID", "Name"),
        show="headings"
    )

    table.heading("ID", text="ID")
    table.heading("Name", text="Name")

    table.column("ID", width=120, anchor="center")
    table.column("Name", width=250, anchor="w")

    # --- Scrollbars ---
    scrollbar_y = ttk.Scrollbar(frame, orient="vertical", command=table.yview)
    scrollbar_x = ttk.Scrollbar(frame, orient="horizontal", command=table.xview)

    table.configure(yscroll=scrollbar_y.set, xscroll=scrollbar_x.set)

    # Layout
    table.grid(row=0, column=0, sticky="nsew")
    scrollbar_y.grid(row=0, column=1, sticky="ns")
    scrollbar_x.grid(row=1, column=0, sticky="ew")

    frame.rowconfigure(0, weight=1)
    frame.columnconfigure(0, weight=1)

    # --- Insert data ---
    for i, j in zip(ID_col, name_col):
        table.insert("", tk.END, values=(i, j))

#IMPORT MAIN GUI ENTRY
def run(parent, connection, database_name):
############################################################################
#Function Name: run
#Description: runs all functions in this script
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: none
############################################################################

    tables = get_table_names(connection, database_name)
    if not tables:
        print("No eligible tables found.")
        return
    
    table_name = choose_table_window(parent, tables)
    if not table_name:
        print("No table selected.")
        return

    ID_col, name_col = get_ID_name_cols(connection, database_name, table_name)

    display_table(parent, ID_col, name_col)