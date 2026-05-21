"""
############################################################################
#Program Filename: Dataq_MychronFormatting_GUI.py
#Author: Mason Allen
#Created Date: 4/5/2026
#Last Updated Date: 4/29/2026
#Description: Converts generated DataQ/MyChron CSV to a CSV formatted for db input
############################################################################
"""

#import all necessary libraries
import pandas as pd
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
from pathlib import Path


def form_entry(parent):
############################################################################
#Function Name: form_entry
#Description: Prompts user to enter a testID for the data file
#Parameters: parent --> parent ttk window
#Return Values: result --> string testID value
############################################################################

    result = {"testID": None}

    window = tk.Toplevel(parent)
    window.title("Data Entry")
    window.transient(parent)
    window.grab_set()
    window.resizable(False, False)

    tk.Label(
        window,
        text="Enter testID"
    ).grid(
        row=0,
        column=0,
        padx=10,
        pady=10
    )

    entry_testid = tk.Entry(window)
    entry_testid.grid(
        row=0,
        column=1,
        padx=10,
        pady=10
    )
    entry_testid.focus_set()


    def submit():
        testID = entry_testid.get().strip()

        if not testID:
            messagebox.showerror(
                "Input Error",
                "testID cannot be empty.",
                parent=window
            )
            return

        result["testID"] = testID
        window.destroy()


    def cancel():
        window.destroy()


    tk.Button(
        window,
        text="Submit",
        command=submit
    ).grid(
        row=1,
        column=0,
        pady=10
    )

    tk.Button(
        window,
        text="Cancel",
        command=cancel
    ).grid(
        row=1,
        column=1,
        pady=10
    )

    parent.wait_window(window)

    return result

def open_file_selection(parent):
############################################################################
#Function Name: open_file_selection
#Description: Prompts user to select a CSV file
#Parameters: parent --> parent ttk window
#Return Values: file_path --> CSV filepath
############################################################################

    try:
        file_path = fd.askopenfilename(
            parent=parent,
            title="Select CSV File",
            filetypes=[("CSV files", "*.csv")]
        )

        if not file_path:
            return None

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        return None

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None

    return file_path

def csv_to_df(file_path, testID):
############################################################################
#Function Name: csv_to_df
#Description: Converts original DataQ/MyChron CSV to db-ready dataframe
#Parameters: file_path --> CSV filepath
#            testID --> testID string
#Return Values: df --> dataframe for new CSV output
############################################################################

    #read CSV data (latin1 is used for unit symbol interpretation)
    df_values = pd.read_csv(
        file_path,
        encoding="latin1"
    )

    #drop rows missing time data
    df_values = df_values.dropna(
        subset=["Time sec"]
    )

    # -----------------------------
    # Sampling-rate detection
    # -----------------------------
    samplingrate = None

    if len(df_values) >= 2:

        delta = (
            df_values["Time sec"].iloc[1]
            - df_values["Time sec"].iloc[0]
        )

        if delta != 0:
            samplingrate = 1 / delta

    print("Detected sampling rate:", samplingrate)


    # -----------------------------
    # Add DB columns
    # -----------------------------
    df_values["deviceID"] = ""
    df_values["testID"] = testID

    # Preserve leading zeros
    df_values["deviceID"] = df_values["deviceID"].astype("string")
    df_values["testID"] = df_values["testID"].astype("string")

    df_values["samplingrate"] = samplingrate


    # Remove optional unused column if present
    if "n/c Volt" in df_values.columns:
        df_values = df_values.drop(
            columns=["n/c Volt"]
        )


    # -----------------------------
    # Required DB column names
    # DO NOT CHANGE
    # -----------------------------
    df = df_values.rename(columns={

        "Time sec": "t_s",
        "Distance km": "my_dist_kft",
        "Engine rpm": "my_rpm",
        "Speed_1 mph": "my_mph",
        "H2O Exit Â°F": "my_h2o_f",
        "EGT Â°F": "my_egt_f",
        "CHT F": "dq_cht_f",
        "Tank T F": "dq_tank_f",
        "Intake T F": "dq_in_air_f",
        "Engine H20 In T F":"dq_e_h2o_f",
        "Pitot kPa": "dq_pitot_kpa",
        "AFR AFR": "dq_afr",
        "Date": "date_attr",
        "Time": "time_of_day",
        "Gear #":"my_gear",
        "Acc_1 g":"my_acc",
        "Datalogger_Temp Â°F":"my_dl_f",
        "Battery V":"my_batt_v"

    })

    return df

def save_output_csv(df, output_file, parent=None):
############################################################################
#Function Name: save_output_csv
#Description: Writes dataframe to CSV and displays success popup
#Parameters: df --> output dataframe
#            output_file --> output csv path
#            parent --> parent ttk window
#Return Values: none
############################################################################

    df.to_csv(
        output_file,
        index=False
    )

    if parent:
        messagebox.showinfo(
            "Success",
            "CSV file created successfully!",
            parent=parent
        )

#IMPORT MAIN GUI ENTRY
def run(parent):
############################################################################
#Function Name: run
#Description: Runs all functions in this script. Allows user to select a data
#             file and enter matching testID.
#Parameters: parent --> parent ttk window
#Return Values: none
############################################################################

    file_path = open_file_selection(parent)

    if not file_path:
        return

    result = form_entry(parent)

    if not result or not result["testID"]:
        return

    testID = result["testID"]

    try:
        df = csv_to_df(
            file_path,
            testID
        )

    except Exception as err:

        messagebox.showerror(
            "Error",
            f"Processing failed:\n{err}",
            parent=parent
        )
        return


    output_path = fd.asksaveasfilename(
        parent=parent,
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv")]
    )

    if not output_path:
        return

    save_output_csv(
        df,
        output_path,
        parent
    )

#CURRENTLY UNUSED AS OF COMMENTING
def run_auto(parent, file_path, folder_path):

    data_file = Path(file_path)

    if not data_file:
        return

    result = form_entry(parent)

    if not result or not result["testID"]:
        return

    testID = result["testID"]

    try:
        df = csv_to_df(
            data_file,
            testID
        )

    except Exception as err:

        messagebox.showerror(
            "Error",
            f"Processing failed:\n{err}",
            parent=parent
        )
        return


    output_folder = Path(folder_path)

    if not output_folder:
        return

    output_file = (
        output_folder /
        "dataq_mychron.csv"
    )

    save_output_csv(
        df,
        output_file,
        parent
    )

#RUN ALL ENTRY
def run_auto_noform(file_path, folder_path, testID, parent=None):
############################################################################
#Function Name: run_auto_noform
#Description: Runs all functions in this script. Starts with a specific data
#             file, output folder path, and testID all chosen. Meant to be
#             used in Run All script. Does not include a form entry for user.
#Parameters: file_path --> original data CSV filepath
#            folder_path --> data folder path
#            testID --> predetermined testID
#            parent --> parent ttk window
#Return Values: output_file --> file path of formatted output CSV
############################################################################

    try:
        df = csv_to_df(
            file_path,
            testID
        )

    except Exception as err:

        if parent:
            messagebox.showerror(
                "Error",
                f"Processing failed:\n{err}",
                parent=parent
            )

        return None


    output_folder = Path(folder_path)

    output_file = (
        output_folder /
        f"dataq_mychron_{testID}.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    return output_file