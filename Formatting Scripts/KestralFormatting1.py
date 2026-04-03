#Author: Mason Allen
#Description: Converts original Kestral csv to csv formatted for data input
#Created Date: 3/1/2026
#Last Updated Date: 3/1/2026

import csv
import mysql.connector
from tkinter import filedialog as fd
from mysql.connector import Error

import pandas as pd
import tkinter as tk
import warnings
from tkinter import messagebox
from datetime import datetime


#CHANGE TO USER INPUT
testID = "000000003"
deviceID = "001"

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection():
   
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(title='Select CSV File', filetypes=[("CSV files","*.csv")])
       
        #if no file is selected, quit the script
        if file_path == None:
            print("No file sected, exiting script...")
            exit()

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        exit()

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        exit()
   
    #return the long format dataframe
    return file_path

def open_csv_file(file_path):
    with open(file_path, mode='r') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)  # Skip the header row
        #for row in reader

def csv_to_df(file_path, deviceID, testID):
    
    df_values = pd.read_csv(file_path, skiprows=[0,1,2,4])
    df_whole = pd.read_csv(file_path, header=None, nrows=3)
    
    name = df_whole.iloc[0, 1]
    model = df_whole.iloc[1, 1]
    serialnum = df_whole.iloc[2, 1]

    df_values["devicename"] = name
    df_values["devicemodel"] = model
    df_values["serialnum"] = serialnum
    df_values["deviceID"] = deviceID
    df_values["testID"] = testID

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
    "Record name": "recordname",
    "Start time": "starttime",
    "Duration (H:M:S)": "duration",
    "Location description": "locationdescription",
    "Location address": "locationaddress",
    "Location coordinates": "locationcoordinates",
    "Notes": "notes"
    })
    #df_headers = pd.read_csv(file_path, skiprows=3, nrows=0)
    #df_headers["devicename"] = pd.Series(dtype="string")
    #df_headers["devicemodel"] = pd.Series(dtype="string")
    #df_headers["serialnum"] = pd.Series(dtype="string")
    
    print(df)
    #print(df_headers)

    return df


file_path = open_file_selection()
df = csv_to_df(file_path,deviceID,testID)
df.to_csv("kestral.csv",index=False)