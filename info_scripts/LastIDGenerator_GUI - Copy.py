"""
############################################################################
#Program Filename: LastIDGenerator_GUI.py
#Author: Mason Allen
#Created Date: 4/28/2026
#Last Updated Date: 5/1/2026
#Description: Allows user to select a table from the db and see the highest
#             non sample ID in that table, and reccomends the next highest
#             ID to use.
############################################################################
"""

#import necessary libraries
import tkinter as tk
from tkinter import ttk, messagebox



def choose_table_windowOLD(parent, tables):

    result = {"table": None}

    win = tk.Toplevel(parent)
    win.title("Select Table")
    win.geometry("380x250")

    tk.Label(
        win,
        text="Select table to view current IDs",
        font=("Segoe UI", 11)
    ).pack(pady=15)

    selected_table = tk.StringVar(win)
    selected_table.set(tables[0])

    dropdown = ttk.Combobox(
        win,
        textvariable=selected_table,
        values=tables,
        state="readonly",
        width=30
    )
    dropdown.pack(pady=10)

    def confirm():
        result["table"] = selected_table.get()
        win.destroy()

    def abort():
        result["table"] = None
        win.destroy()

    button_frame = tk.Frame(win)
    button_frame.pack(pady=15)

    tk.Button(
        button_frame,
        text="Confirm",
        width=12,
        command=confirm
    ).pack(side="left", padx=5)

    tk.Button(
        button_frame,
        text="Abort",
        width=12,
        command=abort
    ).pack(side="left", padx=5)

    win.transient(parent)
    win.grab_set()
    parent.wait_window(win)

    return result["table"]

def choose_table_window(parent, tables):

    result = {"table": None}

    win = tk.Toplevel(parent)
    win.title("Select Table")
    win.transient(parent)
    win.grab_set()
    win.resizable(False, False)

    frame = ttk.Frame(win, padding=20)
    frame.pack(fill="both", expand=True)

    ttk.Label(
        frame,
        text="Select table to view current IDs",
        font=("Segoe UI", 11, "bold")
    ).grid(row=0, column=0, columnspan=2, pady=(0, 15))

    selected_table = tk.StringVar(value=tables[0])

    dropdown = ttk.Combobox(
        frame,
        textvariable=selected_table,
        values=tables,
        state="readonly",
        width=30
    )
    dropdown.grid(row=1, column=0, columnspan=2, pady=10)
    dropdown.focus_set()

    def confirm():
        result["table"] = selected_table.get()
        win.destroy()

    def abort():
        result["table"] = None
        win.destroy()

    win.protocol("WM_DELETE_WINDOW", abort)

    ttk.Button(frame, text="Confirm", command=confirm)\
        .grid(row=2, column=0, pady=10, padx=5)

    ttk.Button(frame, text="Abort", command=abort)\
        .grid(row=2, column=1, pady=10, padx=5)

    parent.wait_window(win)

    return result["table"]

# --------------------------------------------------
# GET VALID TABLES
# --------------------------------------------------

def get_table_names(connection, database_name):

    cursor = connection.cursor()

    query = """
        SELECT DISTINCT table_name
        FROM information_schema.columns
        WHERE table_schema = %s
        AND ordinal_position = 1
        AND column_name LIKE '%%ID'
        AND table_name NOT LIKE 'rel_%%'
        ORDER BY table_name
    """

    cursor.execute(query, (database_name,))
    tables = [row[0] for row in cursor.fetchall()]

    cursor.close()

    return tables


# --------------------------------------------------
# GET ID COLUMN
# --------------------------------------------------

def get_id_column(connection, database_name, table_name):

    cursor = connection.cursor()

    query = """
        SELECT COLUMN_NAME
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s
        AND ordinal_position=1
    """

    cursor.execute(query,(database_name,table_name))

    row = cursor.fetchone()

    cursor.close()

    if not row:
        return None

    return row[0]


# --------------------------------------------------
# GET LAST ID
# --------------------------------------------------

def get_last_id(connection, table_name, id_col):

    cursor = connection.cursor()

    query = f"""
        SELECT `{id_col}`
        FROM `{table_name}`
        WHERE `{id_col}` NOT LIKE '%S%'
        ORDER BY CAST(`{id_col}` AS UNSIGNED) DESC
        LIMIT 1
    """

    cursor.execute(query)

    row = cursor.fetchone()

    cursor.close()

    if row:
        return str(row[0])

    return None


# --------------------------------------------------
# CALCULATE NEXT ID
# --------------------------------------------------

def generate_next_id(last_id):
    """
    Preserve leading zeros.
    Example:
      0004 -> 0005
      005  -> 006
      000000012 -> 000000013
    """

    width = len(last_id)

    next_num = int(last_id.strip()) + 1

    next_id = str(next_num).zfill(width)

    return next_id


# --------------------------------------------------
# DISPLAY RESULT WINDOW
# --------------------------------------------------

def display_id_windowOld(
    parent,
    table_name,
    id_col,
    last_id,
    next_id
):

    win = tk.Toplevel(parent)
    win.title("Current ID Information")
    win.geometry("460x550")

    frame = ttk.Frame(
        win,
        padding=25
    )
    frame.pack(fill="both", expand=True)

    ttk.Label(
        frame,
        text="ID Information",
        font=("Segoe UI",15,"bold")
    ).pack(pady=(0,20))


    ttk.Label(
        frame,
        text=f"Table: {table_name}",
        font=("Segoe UI",11)
    ).pack(pady=4)

    ttk.Label(
        frame,
        text=f"ID Column: {id_col}",
        font=("Segoe UI",11)
    ).pack(pady=4)


    ttk.Separator(frame).pack(
        fill="x",
        pady=18
    )


    ttk.Label(
        frame,
        text="Last Used ID",
        font=("Segoe UI",11)
    ).pack()

    ttk.Label(
        frame,
        text=last_id,
        font=("Consolas",18,"bold")
    ).pack(pady=(5,18))


    ttk.Label(
        frame,
        text="Next Available ID",
        font=("Segoe UI",11)
    ).pack()

    ttk.Label(
        frame,
        text=next_id,
        font=("Consolas",18,"bold")
    ).pack(pady=5)


    ttk.Button(
        frame,
        text="Close",
        command=win.destroy
    ).pack(pady=20)


    win.transient(parent)
    win.grab_set()
    parent.wait_window(win)

def display_id_window(parent, table_name, id_col, last_id, next_id):

    win = tk.Toplevel(parent)
    win.title("Current ID Information")
    win.transient(parent)
    win.grab_set()
    win.resizable(False, False)

    frame = ttk.Frame(win, padding=25)
    frame.pack(fill="both", expand=True)

    ttk.Label(
        frame,
        text="ID Information",
        font=("Segoe UI", 15, "bold")
    ).pack(pady=(0, 20))

    ttk.Label(frame, text=f"Table: {table_name}").pack(pady=3)
    ttk.Label(frame, text=f"ID Column: {id_col}").pack(pady=3)

    ttk.Separator(frame).pack(fill="x", pady=15)

    ttk.Label(frame, text="Last Used ID").pack()
    ttk.Label(frame, text=last_id, font=("Consolas", 18, "bold"))\
        .pack(pady=(5, 15))

    ttk.Label(frame, text="Next Available ID").pack()
    ttk.Label(frame, text=next_id, font=("Consolas", 18, "bold"))\
        .pack(pady=(5, 15))

    ttk.Button(frame, text="Close", command=win.destroy)\
        .pack(pady=15)

    win.protocol("WM_DELETE_WINDOW", win.destroy)

    parent.wait_window(win)

# --------------------------------------------------
# MAIN ENTRY POINT
# --------------------------------------------------

def run(parent, connection, database_name):

    tables = get_table_names(
        connection,
        database_name
    )

    if not tables:
        messagebox.showerror(
            "Error",
            "No valid ID tables found.",
            parent=parent
        )
        return


    table_name = choose_table_window(
        parent,
        tables
    )

    if not table_name:
        print("User cancelled.")
        return


    id_col = get_id_column(
        connection,
        database_name,
        table_name
    )

    if not id_col:
        messagebox.showerror(
            "Error",
            "Could not determine ID column.",
            parent=parent
        )
        return


    last_id = get_last_id(
        connection,
        table_name,
        id_col
    )

    if last_id is None:
        messagebox.showinfo(
            "No Records",
            f"No rows found in {table_name}",
            parent=parent
        )
        return


    next_id = generate_next_id(
        last_id
    )


    display_id_window(
        parent,
        table_name,
        id_col,
        last_id,
        next_id
    )