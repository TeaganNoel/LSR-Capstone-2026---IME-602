#Author: Mason Allen
#Description: For use in main GUI. Converts generated DataQ MyChron3 xlsx to csv formatted for data input
#Created Date: 4/5/2026
#Last Updated Date: 4/5/2026

import pandas as pd
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox

# 🔹 Default values (you can later replace with user input fields)
testID = 999999999
deviceID = 999
segment = 5


# 🔹 File picker (NOW uses parent)
def open_file_selection(parent):
    
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(
            parent=parent,
            title='Select CSV File',
            filetypes=[("CSV files", "*.csv")]
        )

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        exit()

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()

    if not file_path:
        print("No file sected, exiting script...")
        return None

    return file_path


def csv_to_df(file_path, deviceID, testID, segment):

    df_whole = pd.read_csv(file_path)

    sampling_rate = (df_whole.iloc[1, 0] - df_whole.iloc[1, 2]) ** (-1)
    print("sampling rate:", sampling_rate)

    df_whole["deviceID"] = deviceID
    df_whole["testID"] = testID
    df_whole["segment"] = segment
    df_whole["samplingrate"] = sampling_rate

    df = df_whole.rename(columns={
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
        "Record name": "recordname",
        "Start time": "starttime",
        "Duration (H:M:S)": "duration",
        "Location description": "locationdescription",
        "Location address": "locationaddress",
        "Location coordinates": "locationcoordinates",
        "Notes": "notes"
    })

    return df


# 🔹 MAIN ENTRY POINT (for GUI button)
def run(parent):

    # Step 1: file selection
    file_path = open_file_selection(parent)

    if not file_path:
        return

    # Step 2: process file
    df = csv_to_df(file_path, deviceID, testID, segment)

    # Step 3: save output
    #output_path = "dataq_mychron3_data.csv"
    output_path = fd.asksaveasfilename(parent=parent, defaultextension=".csv")
    df.to_csv(output_path, index=False)

    # Step 4: notify user
    messagebox.showinfo(
        "Success",
        f"File processed and saved as:\n{output_path}",
        parent=parent
    )