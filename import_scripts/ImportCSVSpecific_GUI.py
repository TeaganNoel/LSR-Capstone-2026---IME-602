#!/usr/bin/env python3

#Author: Mason Allen
#Description: Made for connection with Main GUI. Imports a selected csv into a table in MySQL using mysql.connector
#Created Date: 4/5/2026
#Last Updated Date: 4/15/2026

import mysql.connector
import pandas as pd
import tkinter as tk
from tkinter import messagebox
from datetime import datetime
from pathlib import Path
import argparse

# ----------------------------- HELPERS ----------------------------- #

def csv_to_df(file_path):
    df = pd.read_csv(file_path)

    print("\nLoaded DataFrame:\n", df, "\n")

    return df


# ----------------------------- CORE IMPORT ----------------------------- #
def import_data(connection, table_name, df, parent=None):
    cursor = connection.cursor()

    try:
        # Replace NaN values in data
        df = df.where(pd.notnull(df), None)

        # 🔍 Detect bad columns
        bad_cols = [
            col for col in df.columns
            if pd.isna(col) or str(col).strip() == ""
        ]

        if bad_cols:
            print("⚠️ Dropping invalid columns:", bad_cols)

            # Drop them
            df = df.loc[:, [
                not (pd.isna(col) or str(col).strip() == "")
                for col in df.columns
            ]]

        # Clean remaining column names
        df.columns = [str(col).strip() for col in df.columns]

        print("Final columns being sent to SQL:", df.columns.tolist())

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

        if parent:
            messagebox.showinfo("Success", "Data imported successfully", parent=parent)

        return True

    except mysql.connector.Error as err:
        print(f"Database error: {err}")

        if parent:
            messagebox.showerror("Import Failed", str(err), parent=parent)

        return False

    except Exception as err:
        print(f"Unexpected error: {err}")

        if parent:
            messagebox.showerror("Error", str(err), parent=parent)

        return False

    finally:
        cursor.close()


def import_data_OLD(connection, table_name, df, parent=None):
    cursor = connection.cursor()

    try:
        df = df.where(pd.notnull(df), None)
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

        if parent:
            messagebox.showinfo("Success", "Data imported successfully", parent=parent)
        return True
    
    except mysql.connector.Error as err:
        print(f"Database error: {err}")

        if parent:
            messagebox.showerror("Import Failed", str(err), parent=parent)
        return False
    
    except Exception as err:
        print(f"Unexpected error: {err}")

        if parent:
            messagebox.showerror("Error", str(err), parent=parent)
        return False
    
    finally:
        cursor.close()


# ----------------------------- MAIN RUN FUNCTION ----------------------------- #

def run(parent, connection, database_name, file_path, table_name):
    """
    GUI-safe entry point (no prompts)
    """

    print("Running CSV import...")
    print("File:", file_path)
    print("Table:", table_name)

    if not file_path or not table_name:
        messagebox.showerror("Error", "File path and table name are required.", parent=parent)
        return False   

    try:
        df = csv_to_df(file_path)

        if df is not None:
            #import_data(connection, table_name, df, parent)
            return import_data(connection, table_name, df, parent)
        
    except Exception as e:
        print("Run error:", e)
        messagebox.showerror("Error", str(e), parent=parent)
        return False

# ----------------------------- CLI ENTRY ----------------------------- #

def main():
    parser = argparse.ArgumentParser(description="Import CSV into MySQL table.")
    parser.add_argument("file_path", type=Path, help="Path to CSV file")
    parser.add_argument("table_name", type=str, help="Target table name")

    args = parser.parse_args()

    # ⚠️ You must plug in your connection here
    import mysql.connector
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="your_password",
        database="your_database"
    )

    # No GUI parent in CLI mode
    run(None, connection, None, args.file_path, args.table_name)


if __name__ == "__main__":
    main()