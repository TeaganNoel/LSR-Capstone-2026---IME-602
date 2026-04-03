#Author: Mason Allen
#Description: Creates and exports CSV files from specified Excel sheets 
#Created Date: 3/5/2026
#Last Updated Date: 3/6/2026

from tkinter import filedialog as fd
import pandas as pd
from datetime import datetime
from pathlib import Path

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection():
   
    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(title='Select Excel File', filetypes=[("Excel files","*.xlsx")])
       
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

#excel_file = "OSU_LSR_DATA_ENTRY_XXXXXX_TEMPLATE_V2[MasonEdits].xlsx"
excel_file = open_file_selection()

# Output directory
output_folder = Path("C:/Users/LSR/Documents/LSR Database Design/Testing Database/Data_CSVs")

# Sheet names and output file prefixes
exports = {
    "Export_CSV_test": "test_Data_generated",
    "Export_CSV_rel_device_test": "rel_data_collection_device_test_Data_generated",
    "Export_CSV_rel_media_test": "rel_media_test_Data_generated",
    "Export_CSV_rel_prototype_test": "rel_prototype_test_Data_generated",
    "Export_CSV_rel_condition_test": "rel_condition_Data_generated"
}

# Create timestamp
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

for sheet, prefix in exports.items():
    df = pd.read_excel(excel_file, sheet_name=sheet)

    #csv_file = f"{prefix}_{timestamp}.csv"
    csv_file = output_folder / f"{prefix}_{timestamp}.csv"
    df.to_csv(csv_file, index=False)

    print(f"CSV created: {csv_file}")