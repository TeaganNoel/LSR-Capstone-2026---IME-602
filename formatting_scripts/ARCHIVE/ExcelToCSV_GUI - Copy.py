#Author: Mason Allen
#Description: Made for use in main GUI. Creates and exports CSV files from specified Excel sheets 
#Created Date: 4/5/2026
#Last Updated Date: 4/21/2026

import pandas as pd
from tkinter import filedialog as fd
from tkinter import messagebox
from datetime import datetime
from pathlib import Path

#prompts user with a file selection window to choose a CSV. Converts CSV to a long dataframe format
def open_file_selection(parent):

    #try opening file and converting to df. Throw error message on exception
    try:
        file_path = fd.askopenfilename(
            parent=parent,
            title='Select Excel File',
            filetypes=[("Excel files", "*.xlsx")]
        )
    
    except ImportError:
        #CHANGE TO MESSAGE BOX
        print("Please install openpyxl: pip install openpyxl")
        return None

    except FileNotFoundError:
        #CHANGE TO MESSAGE BOX
        print(f"Error: The file '{file_path}' was not found.")
        return None

    if not file_path:
        print("No file selected, exiting script...")
        return None

    return file_path


def run(parent):

    # Step 1: select Excel file
    excel_file = open_file_selection(parent)
    if not excel_file:
        return

    # Step 2: choose output folder
    output_folder = fd.askdirectory(parent=parent, title="Select Output Folder")
    if not output_folder:
        return

    output_folder = Path(output_folder)

    # Step 3: load Excel file
    try:
        xls = pd.ExcelFile(excel_file)
    except Exception as err:
        messagebox.showerror("Error", f"Failed to open Excel file:\n{err}", parent=parent)
        return

    # Step 4: filter sheets
    export_sheets = [s for s in xls.sheet_names if s.startswith("Export_CSV_")]

    if not export_sheets:
        messagebox.showwarning(
            "No Sheets Found",
            "No sheets starting with 'Export_CSV_' were found.",
            parent=parent
        )
        return

    # Step 5: timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    created_files = []

    try:
        for sheet in export_sheets:
            df = pd.read_excel(xls, sheet_name=sheet, dtype=str)

            # remove prefix for cleaner filename
            prefix = sheet.replace("Export_CSV_", "")

            csv_file = output_folder / f"{prefix}_{timestamp}.csv"
            df.to_csv(csv_file, index=False)

            created_files.append(str(csv_file))

    except Exception as err:
        messagebox.showerror("Error", f"Failed during conversion:\n{err}", parent=parent)
        return

    # Step 6: success message
    messagebox.showinfo(
        "Success",
        f"CSV files created:\n\n" + "\n".join(created_files),
        parent=parent
    )


def run_auto(parent, file_path, folder_path):

    # Step 1: select Excel file
    excel_file = file_path
    if not excel_file:
        return

    # Step 2: choose output folder
    
    if not folder_path:
        return
    
    output_folder = Path(folder_path)

    # Step 3: load Excel file
    try:
        xls = pd.ExcelFile(excel_file)
    except Exception as err:
        messagebox.showerror("Error", f"Failed to open Excel file:\n{err}", parent=parent)
        return

    # Step 4: filter sheets
    export_sheets = [s for s in xls.sheet_names if s.startswith("Export_CSV_")]

    if not export_sheets:
        messagebox.showwarning(
            "No Sheets Found",
            "No sheets starting with 'Export_CSV_' were found.",
            parent=parent
        )
        return

    # Step 5: timestamp
    #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    created_files = []

    for sheet in export_sheets:
        try:
        
            df = pd.read_excel(xls, sheet_name=sheet,dtype=str)

            # remove prefix for cleaner filename
            prefix = sheet.replace("Export_CSV_", "")

            csv_file = output_folder / f"{prefix}.csv"
            df.to_csv(csv_file, index=False)

            created_files.append(str(csv_file))

        except Exception as err:
            messagebox.showerror("Error", f"Failed during conversion:\n{err}", parent=parent)
            return

    # Step 6: success message
    messagebox.showinfo(
        "Success",
        f"CSV files created:\n\n" + "\n".join(created_files),
        parent=parent
    )    