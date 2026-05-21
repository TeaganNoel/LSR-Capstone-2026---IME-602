"""
############################################################################
#Program Filename: ImportCSVSpecific_GUI.py
#Author: Mason Allen
#Created Date: 4/5/2026
#Last Updated Date: 5/15/2026
#Description: Imports a specific CSV into a specified db table using
#             mysql.connector. Made for connection with Run All scripts
#             and automated imports.
############################################################################
"""

#import necessary libraries
import mysql.connector
import pandas as pd
from tkinter import messagebox
from pathlib import Path
import argparse


def csv_to_df(file_path):
############################################################################
#Function Name: csv_to_df
#Description: Reads and converts CSV to dataframe
#Parameters: file_path --> file path of selected csv
#Return Values: df --> dataframe of csv data
############################################################################

    df = pd.read_csv(
        file_path,
        dtype=str
    )

    print("\nLoaded DataFrame:\n", df, "\n")
    print("RAW columns:", list(df.columns))

    return df

def import_data(connection, table_name, df, parent=None):
############################################################################
#Function Name: import_data
#Description: Connects to the database using a MySQL Connector cursor and
#             imports CSV data into the selected table using SQL.
#Parameters: connection --> MySQL db connection for queries and imports
#            table_name --> name of table to import into
#            df --> data frame of values to be imported to the db table
#            parent --> parent ttk window
#Return Values: success --> T/F import status
#               error_msg --> import error text if import fails
############################################################################

    cursor = connection.cursor()

    try:

        # Clean null-like values
        df = df.replace("nan", None)
        df = df.replace("", None)

        # Convert NaN properly to None
        df = df.astype(object).where(
            pd.notnull(df),
            None
        )

        # Ensure column names are strings
        df.columns = df.columns.astype(str)

        print(
            "Final columns being sent to SQL:",
            df.columns.tolist()
        )

        # -----------------------------
        # Build SQL statement
        # -----------------------------
        columns = ', '.join(df.columns)

        placeholders = ', '.join(
            ['%s'] * len(df.columns)
        )

        sql = f"""
            INSERT INTO {table_name}
            ({columns})
            VALUES ({placeholders})
        """

        # -----------------------------
        # Fast dataframe conversion
        # -----------------------------
        data = list(
            df.itertuples(
                index=False,
                name=None
            )
        )

        # -----------------------------
        # Execute SQL import
        # -----------------------------
        cursor.executemany(sql, data)

        connection.commit()

        print("Data imported successfully")

        if parent:
            messagebox.showinfo(
                "Success",
                "Data imported successfully",
                parent=parent
            )

        return True, None

    except mysql.connector.DataError as err:

        error_msg = (
            f"Wrong data type in CSV.\n\n"
            f"Error:\n{err}"
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

    except Exception as err:

        error_msg = (
            f"Unexpected error:\n\n{err}"
        )

        print(error_msg)

        return False, error_msg

    finally:
        cursor.close()


#RUN ALL / AUTOMATED IMPORT ENTRY
def run(parent, connection, file_path, table_name):
############################################################################
#Function Name: run
#Description: Main function to run all other functions in script. This is
#             the entry point from Run All scripts and automated imports.
#             Uses a predetermined CSV file path and table name.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            file_path --> CSV file path
#            table_name --> target db table name
#Return Values: success --> T/F import status
############################################################################

    print("Running CSV import...")
    print("File:", file_path)
    print("Table:", table_name)

    if not file_path or not table_name:

        if parent:
            messagebox.showerror(
                "Error",
                "File path and table name are required.",
                parent=parent
            )

        return False

    try:

        df = csv_to_df(file_path)

        if df is not None:

            success, error_msg = import_data(
                connection,
                table_name,
                df,
                parent
            )

            if not success:

                if parent:
                    messagebox.showerror(
                        "Import Failed",
                        error_msg,
                        parent=parent
                    )

                return False

            return True

    except Exception as err:

        print("Run error:", err)

        if parent:
            messagebox.showerror(
                "Error",
                str(err),
                parent=parent
            )

        return False