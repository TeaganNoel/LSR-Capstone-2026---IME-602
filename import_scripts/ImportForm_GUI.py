#Author: Mason Allen
#Description: Made for connection with Main GUI. imports a single entity based on a form input
#Created Date: 4/4/2026
#Last Updated Date: 4/4/2026

import pandas as pd
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
import warnings

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


def form_query(parent, fields):
    entry_window = tk.Toplevel(parent)
    entry_window.title("Data Entry Form")

    entries = {}
    df = None

    for i, field in enumerate(fields):
        tk.Label(entry_window, text=field["label"]).grid(row=i, column=0)
        e = tk.Entry(entry_window)
        e.grid(row=i, column=1)
        entries[field["name"]] = e

    def submit():
        nonlocal df
        data = {}

        for field in fields:
            raw = entries[field["name"]].get()

            if raw == "":
                messagebox.showerror("Error", f"{field['label']} cannot be empty.")
                return

            try:
                value = field["type"](raw)
            except ValueError:
                messagebox.showerror("Error", f"{field['label']} invalid type.")
                return

            data[field["name"]] = [value]

        df = pd.DataFrame(data)
        entry_window.destroy()

    tk.Button(entry_window, text="Import", command=submit).grid(row=len(fields), column=1)
    tk.Button(entry_window, text='Quit', command=entry_window.destroy).grid(row=len(fields), column=0, pady=4)

    parent.wait_window(entry_window)
    return df


def import_data(connection, table_name, df):
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

    except Exception as err:
        print(f"Import failed: {err}")

    cursor.close()


# 🔹 MAIN ENTRY POINT
def run(parent, connection, database_name):

    tables = get_table_names(connection, database_name)

    table_name = choose_table_window(parent, tables)
    if not table_name:
        return

    schema = get_table_schema(connection, database_name, table_name)

    fields = build_fields_from_schema(schema)

    df = form_query(parent, fields)

    if df is not None:
        import_data(connection, table_name, df)