#Author: Mason Allen
#Description: Converts generated DataQ MyChron3 xlsx to csv formatted for data input
#Created Date: 3/5/2026
#Last Updated Date: 3/6/2026

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
testID = 999999999
deviceID = 999
segment = 5 
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

def csv_to_df(file_path, deviceID, testID,segment):
    
    df_whole = pd.read_csv(file_path)
    sampling_rate = (df_whole.iloc[1,0] - df_whole.iloc[1,2])**(-1)
    print("sampling rate:",sampling_rate)

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
    #df_headers = pd.read_csv(file_path, skiprows=3, nrows=0)
    #df_headers["devicename"] = pd.Series(dtype="string")
    #df_headers["devicemodel"] = pd.Series(dtype="string")
    #df_headers["serialnum"] = pd.Series(dtype="string")
    
    print(df)
    #print(df_headers)

    return df


file_path = open_file_selection()
df = csv_to_df(file_path,deviceID,testID,segment)
df.to_csv("dataq_mychron3_data.csv",index=False)