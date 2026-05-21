"""
############################################################################
#Program Filename: KestrelFormatting_GUI.py
#Author: Mason Allen
#Created Date: 4/5/2026
#Last Updated Date: 4/29/2026
#Description: Converts original Kestral output CSV to a CSV formatted for db input
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
#Description: Prompts user to enter a testID for the Kestrel file
#Parameters: parent --> parent ttk window
#Return Values: result --> string testID value with 9 characters
############################################################################

    result = {"testID": None}

    window = tk.Toplevel(parent)
    window.title("Data Entry")
    window.transient(parent)
    window.grab_set()          # stronger modal behavior
    window.resizable(False, False)

    tk.Label(window, text="Enter testID").grid(row=0, column=0, padx=10, pady=10)
    entry_testid = tk.Entry(window)
    entry_testid.grid(row=0, column=1, padx=10, pady=10)
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

    tk.Button(window, text="Submit", command=submit).grid(row=1, column=0, pady=10)
    tk.Button(window, text="Cancel", command=cancel).grid(row=1, column=1, pady=10)

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
#Description: Prompts user to select a CSV file
#Parameters: file_path --> CSV filepath
#            testID --> testID string
#Return Values: df --> dataframe for new CSV output
############################################################################

    #read CSV data
    df_values = pd.read_csv(
        file_path,
        skiprows=[0, 1, 2, 4]
    )

    #read CSV metadata
    df_meta = pd.read_csv(
        file_path,
        header=None,
        nrows=3
    )

    # Convert datetime column
    df_values["FORMATTED DATE_TIME"] = pd.to_datetime(
        df_values["FORMATTED DATE_TIME"],
        errors="coerce"
    )

    # Drop invalid rows
    df_values = df_values.dropna(
        subset=["FORMATTED DATE_TIME"]
    )

    # -----------------------------
    # Sampling-rate detection
    # -----------------------------
    samplingrate = None

    if len(df_values) >= 2:
        delta = (
            df_values["FORMATTED DATE_TIME"].iloc[1]
            - df_values["FORMATTED DATE_TIME"].iloc[0]
        )

        secs = delta.total_seconds()

        if secs != 0:
            samplingrate = 1 / secs

    print("Detected sampling rate:", samplingrate)

    # -----------------------------
    # Metadata extraction
    # -----------------------------
    name = df_meta.iloc[0, 1]
    model = df_meta.iloc[1, 1]
    serialnum = df_meta.iloc[2, 1]

    recordname = df_values.iloc[0, 17]
    starttime = df_values.iloc[0, 18]
    duration = df_values.iloc[0, 19]
    locationdescription = df_values.iloc[0, 20]
    locationaddress = df_values.iloc[0, 21]
    locationcoordinates = df_values.iloc[0, 22]
    notes = df_values.iloc[0, 23]

    # -----------------------------
    # Add DB columns
    # -----------------------------
    df_values["devicename"] = name
    df_values["devicemodel"] = model
    df_values["serialnum"] = serialnum

    df_values["deviceID"] = "001"
    df_values["testID"] = testID

    # Preserve leading zeros
    df_values["deviceID"] = df_values["deviceID"].astype("string")
    df_values["testID"] = df_values["testID"].astype("string")

    df_values["samplingrate"] = samplingrate

    df_values["recordname"] = recordname
    df_values["starttime"] = starttime
    df_values["duration"] = duration
    df_values["locationdescription"] = locationdescription
    df_values["locationaddress"] = locationaddress
    df_values["locationcoordinates"] = locationcoordinates
    df_values["notes"] = notes

    # Remove redundant originals
    df_values = df_values.drop(
        columns=[
            "Record name",
            "Start time",
            "Duration (H:M:S)",
            "Location description",
            "Location address",
            "Location coordinates",
            "Notes"
        ]
    )

    # -----------------------------
    # Required DB column names
    # DO NOT CHANGE
    # -----------------------------
    df = df_values.rename(columns={
        "FORMATTED DATE_TIME": "datetime_attr",
        "Temperature": "temperature",
        "Wet Bulb Temp": "wetbulbtemp",
        "Relative Humidity": "relativehumidity",
        "Barometric Pressure": "barometricpressure",
        "Altitude": "altitude",
        "Station Pressure": "stationpressure",
        "Wind Speed": "windspeed",
        "Heat Index": "heatindex",
        "Dew Point": "dewpointtemp",
        "Density Altitude": "densityaltitude",
        "Crosswind": "crosswind",
        "Headwind": "headwind",
        "Compass Magnetic Direction": "compassmagdirection",
        "Compass True Direction": "compasstruedirection",
        "Wind Chill": "windchill",
        "Data Type": "datatype",
    })

    return df

#IMPORT MAIN GUI ENTRY
def run(parent):
############################################################################
#Function Name: run
#Description: Runs all functions in this script. Allows user to select a kestrel
#             file and enter the matching testID.
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

    df.to_csv(output_path, index=False)

    messagebox.showinfo(
        "Success",
        "CSV file created successfully!",
        parent=parent
    )

#CURRENTLY UNUSED AS OF COMMENTING
def run_auto(parent, file_path, folder_path):

    kestrel_file = Path(file_path)
    if not kestrel_file:
        return

    result = form_entry(parent)
    if not result or not result["testID"]:
        return

    testID = result["testID"]

    try:
        df = csv_to_df(
            kestrel_file,
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

    output_file = output_folder / "kestrel.csv"

    df.to_csv(
        output_file,
        index=False
    )

    messagebox.showinfo(
        "Success",
        "CSV file created successfully!",
        parent=parent
    )

#RUN ALL ENTRY 
def run_auto_noform(file_path, folder_path, testID, parent=None):
############################################################################
#Function Name: run_auto_noform
#Description: Runs all functions in this script. Starts with a specific Kestrel
#             file, output folder path, and testID all chosen. Meant to be used
#             in Run All script. Does not include a form entry for user.
#Parameters: file_path --> original Kestrel CSV file path
#            folder_path --> data folder path
#            testID --> predetermined testID based on Kestrel CSV filename
#            parent --> parent ttk window
#Return Values: output_file --> file path of formatted Kestrel CSV
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
        f"kestrel_{testID}.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    return output_file