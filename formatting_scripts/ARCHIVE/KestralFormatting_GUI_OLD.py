#Author: Mason Allen
#Description: Made for use in main GUI. Converts original Kestral csv to csv formatted for data input
#Created Date: 4/5/2026
#Last Updated Date: 4/5/2026

import csv
from tkinter import filedialog as fd
import pandas as pd
import tkinter as tk
from tkinter import messagebox
import sys

#CHANGE TO USER INPUT
testID = "sample"
deviceID = "sample"
samplingrate = 0 #1 record every 2 seconds
###############

def form_entry(parent):
    result = {"data": None}  # container to hold output

    def submit():
        print("SUBMIT CLICKED")

        testID = entry_testid.get().strip()
        deviceID = entry_deviceid.get().strip()
        samplingrate = entry_sampling.get().strip()

        if not testID or not deviceID:
            print("FAILED: empty fields")
            messagebox.showerror("Input Error", "testID and deviceID cannot be empty.")
            return

        try:
            samplingrate_val = float(samplingrate)
        except ValueError:
            print("FAILED: invalid float:", samplingrate)
            messagebox.showerror("Input Error", "samplingrate must be a number.")
            return

        print("SUCCESS: storing result")

        result["data"] = (testID, deviceID, samplingrate_val)
    
        window.destroy()
        return result

    def quit_app():
        window.destroy()
        return

    # Create window
    window = tk.Toplevel(parent)
    window.title("Data Entry")

    # Labels + Entries
    tk.Label(window, text="testID").grid(row=0, column=0, padx=5, pady=5)
    entry_testid = tk.Entry(window)
    entry_testid.grid(row=0, column=1, padx=5, pady=5)

    tk.Label(window, text="deviceID").grid(row=1, column=0, padx=5, pady=5)
    entry_deviceid = tk.Entry(window)
    entry_deviceid.grid(row=1, column=1, padx=5, pady=5)

    tk.Label(window, text="samplingrate").grid(row=2, column=0, padx=5, pady=5)
    entry_sampling = tk.Entry(window)
    entry_sampling.grid(row=2, column=1, padx=5, pady=5)

    # Buttons
    tk.Button(window, text="Submit", command=submit).grid(row=3, column=0, pady=10)
    tk.Button(window, text="Quit", command=quit_app).grid(row=3, column=1, pady=10)

    window.mainloop()
    return result
###############


#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection(parent):
   
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(parent=parent, title='Select CSV File', filetypes=[("CSV files","*.csv")])
       
        #if no file is selected, quit the script
        if not file_path:
            print("No file sected, exiting script...")
            return None

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        return None

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
   
    #return the long format dataframe
    return file_path

def open_csv_file(file_path):
    with open(file_path, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        #for row in reader

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
    #"Record name": "recordname",
    #"Start time": "starttime",
    #"Duration (H:M:S)": "duration",
    #"Location description": "locationdescription",
    #"Location address": "locationaddress",
    #"Location coordinates": "locationcoordinates",
    #"Notes": "notes"
    })
    #df_headers = pd.read_csv(file_path, skiprows=3, nrows=0)
    #df_headers["devicename"] = pd.Series(dtype="string")
    #df_headers["devicemodel"] = pd.Series(dtype="string")
    #df_headers["serialnum"] = pd.Series(dtype="string")
    #print("DF VALUES:")
    #print(df_values)
    #print()
    #print(df)
    #print(df_headers)

    return df


# 🔹 MAIN ENTRY POINT
def run(parent):
    
    file_path = open_file_selection(parent)
    result = form_entry(parent)
    print("result:",result)

    if result:
        testID, deviceID, samplingrate = result["data"]
        print("DEBUG 2")
        print(testID, deviceID, samplingrate)
    else:
        print("User cancelled")

    print("DEBUG 3")
    print("testID:", testID)
    print("deviceID:", deviceID)
    print("samplingrate:", samplingrate)

    df = csv_to_df(file_path,deviceID,testID,samplingrate)
    
    output_path = fd.asksaveasfilename(parent=parent, defaultextension=".csv")
    df.to_csv(output_path, index=False)
