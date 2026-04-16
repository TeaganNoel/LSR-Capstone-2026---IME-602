#Author: Mason Allen
#Description: Made for use in main GUI. Converts original Kestral csv to csv formatted for data input
#Created Date: 4/5/2026
#Last Updated Date: 4/6/2026

import pandas as pd
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
from pathlib import Path


def form_entry(parent):
    result = {"data": None}

    window = tk.Toplevel(parent)
    window.title("Data Entry")

    tk.Label(window, text="testID").grid(row=0, column=0, padx=5, pady=5)
    entry_testid = tk.Entry(window)
    entry_testid.grid(row=0, column=1)

    tk.Label(window, text="deviceID").grid(row=1, column=0, padx=5, pady=5)
    entry_deviceid = tk.Entry(window)
    entry_deviceid.grid(row=1, column=1)

    tk.Label(window, text="samplingrate").grid(row=2, column=0, padx=5, pady=5)
    entry_sampling = tk.Entry(window)
    entry_sampling.grid(row=2, column=1)

    def submit():
        testID = entry_testid.get().strip()
        deviceID = entry_deviceid.get().strip()
        samplingrate = entry_sampling.get().strip()

        if not testID or not deviceID:
            messagebox.showerror("Input Error", "testID and deviceID cannot be empty.", parent=window)
            return

        try:
            samplingrate_val = float(samplingrate)
        except ValueError:
            messagebox.showerror("Input Error", "samplingrate must be a number.", parent=window)
            return

        result["data"] = (testID, deviceID, samplingrate_val)
        window.destroy()

    def cancel():
        window.destroy()

    tk.Button(window, text="Submit", command=submit).grid(row=3, column=0, pady=10)
    tk.Button(window, text="Cancel", command=cancel).grid(row=3, column=1, pady=10)

    parent.wait_window(window)  # ✅ CRITICAL FIX

    return result


def open_file_selection(parent):
    try:
        file_path = fd.askopenfilename(
            parent=parent,
            title='Select CSV File',
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


def csv_to_df(file_path, deviceID, testID, samplingrate):

    df_values = pd.read_csv(file_path, skiprows=[0,1,2,4])
    df_meta = pd.read_csv(file_path, header=None, nrows=3)

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

    df_values["devicename"] = name
    df_values["devicemodel"] = model
    df_values["serialnum"] = serialnum
    df_values["deviceID"] = deviceID
    df_values["testID"] = testID
    
#new column fixes
    df_values["samplingrate"] = samplingrate

    df_values["recordname"] = recordname
    df_values["starttime"] = starttime
    df_values["duration"] = duration
    df_values["locationdescription"] = locationdescription
    df_values["locationaddress"] = locationaddress
    df_values["locationcoordinates"] = locationcoordinates
    df_values["notes"] = notes

    df_values = df_values.drop(columns=['Record name', 'Start time','Duration (H:M:S)','Location description','Location address','Location coordinates','Notes'])

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


# 🔹 MAIN ENTRY POINT
def run(parent):

    # Step 1: file selection
    file_path = open_file_selection(parent)
    if not file_path:
        return

    # Step 2: form input
    result = form_entry(parent)
    if not result or not result["data"]:
        return

    testID, deviceID, samplingrate = result["data"]

    # Step 3: process
    try:
        df = csv_to_df(file_path, deviceID, testID, samplingrate)
    except Exception as err:
        messagebox.showerror("Error", f"Processing failed:\n{err}", parent=parent)
        return

    # Step 4: save file
    output_path = fd.asksaveasfilename(
        parent=parent,
        defaultextension=".csv",
        filetypes=[("CSV files", "*.csv")]
    )

    if not output_path:
        return

    df.to_csv(output_path, index=False)

    messagebox.showinfo("Success", "CSV file created successfully!", parent=parent)

# 🔹 RUN ALL ENTRY POINT
def run_auto(parent, file_path, folder_path):

    # Step 1: file selection
    kestrel_file = Path(file_path)
    if not kestrel_file:
        return

    # Step 2: form input
    result = form_entry(parent)
    if not result or not result["data"]:
        return

    testID, deviceID, samplingrate = result["data"]

    # Step 3: process
    try:
        df = csv_to_df(kestrel_file, deviceID, testID, samplingrate)
    except Exception as err:
        messagebox.showerror("Error", f"Processing failed:\n{err}", parent=parent)
        return

    # Step 4: save file
    output_folder = Path(folder_path)
    if not output_folder:
        return

    df.to_csv(output_folder, index=False)

    messagebox.showinfo("Success", "CSV file created successfully!", parent=parent)