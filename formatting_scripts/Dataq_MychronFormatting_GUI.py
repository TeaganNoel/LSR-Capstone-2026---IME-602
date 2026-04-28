#Author: Mason Allen
#Description: For use in main GUI. Converts generated DataQ MyChron3 xlsx to csv formatted for data input
#Created Date: 4/5/2026
#Last Updated Date: 4/22/2026

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

    def submit():
        testID = entry_testid.get().strip()


        if not testID:
            messagebox.showerror("Input Error", "testID cannot be empty.", parent=window)
            return

        result["data"] = (testID)
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

def csv_to_df(file_path, testID):

    df_values = pd.read_csv(file_path, encoding="latin1")
    #df_meta = pd.read_csv(file_path, header=None, nrows=3)

    # Convert datetime column
    #df_values["Time sec"] = pd.to_datetime(df_values["Time sec"], errors="coerce")

    # Drop bad rows if needed
    df_values = df_values.dropna(subset=["Time sec"])

    # Detect sampling rate from first two timestamps
    if len(df_values) >= 2:
        delta = df_values["Time sec"].iloc[1] - df_values["Time sec"].iloc[0]
        samplingrate = 1 / delta
    else:
        samplingrate = None

    print("Detected sampling rate:", samplingrate)

    df_values["deviceID"] = ""
    df_values["testID"] = testID
    df_values["deviceID"] = df_values["deviceID"].astype("string")
    df_values["testID"] = df_values["testID"].astype("string")
    
    #new column fixes
    df_values["samplingrate"] = samplingrate

    try:
        df_values = df_values.drop(columns=['n/c Volt'])
    except:
        return

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

    testID = result["data"]

    # Step 3: process
    try:
        df = csv_to_df(file_path, testID)
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
    data_file = Path(file_path)
    if not data_file:
        return

    # Step 2: form input
    result = form_entry(parent)
    if not result or not result["data"]:
        return

    testID, deviceID, samplingrate = result["data"]

    # Step 3: process
    try:
        df = csv_to_df(data_file, testID)
    except Exception as err:
        messagebox.showerror("Error", f"Processing failed:\n{err}", parent=parent)
        return

    # Step 4: save file
    output_folder = Path(folder_path)
    if not output_folder:
        return

    df.to_csv(output_folder, index=False)

    messagebox.showinfo("Success", "CSV file created successfully!", parent=parent)


def run_auto_noform(file_path, folder_path, testID, parent=None):
    
    try:
        df = csv_to_df(file_path, testID)
    
    except Exception as err:
        if parent:
            messagebox.showerror("Error", f"Processing failed:\n{err}", parent=parent)
        return None

    output_folder = Path(folder_path)
    output_file = output_folder / f"dataq_mychron_{testID}.csv"

    df.to_csv(output_file, index=False)

    return output_file