"""
############################################################################
Program Filename: ExcelToCSV_GUI.py 
Author: Mason Allen 
Created Date: 4/5/2026 
Last Updated Date: 4/29/2026 
Description: This script turns all sheets in an Excel workbook with a specific
            naming convention (Export_CSV_...) into individual CSV files. 
            This script is made to function with the import main GUI. 
############################################################################
"""

#import all necessary libraries
import pandas as pd
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
from tkinter import ttk
from datetime import datetime
from pathlib import Path


def open_file_selection(parent):
############################################################################
#Function Name: open_file_selection
#Description: Prompt user to select Excel file.
#Parameters: parent --> parent ttk window
#Return Values: file_path --> file path of selected file
############################################################################

    try:
        file_path = fd.askopenfilename(
            parent=parent,
            title='Select Excel File',
            filetypes=[("Excel files", "*.xlsx")]
        )

    except ImportError:
        print("Please install openpyxl: pip install openpyxl")
        return None

    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None

    if not file_path:
        print("No file selected, exiting script...")
        return None

    return file_path

def open_output_folder(parent):
############################################################################
#Function Name: open_output_folder
#Description: Prompt user to select output folder for CSVs.
#Parameters: parent --> parent ttk window
#Return Values: Path(folder) --> folder path of selected folder
############################################################################

    folder = fd.askdirectory(
        parent=parent,
        title="Select Output Folder"
    )

    if not folder:
        return None

    return Path(folder)

def load_excel_file(parent, excel_file):
############################################################################
#Function Name: load_excel_file
#Description: Load Excel workbook with openpyxl.
#Parameters: parent --> parent ttk window
#            excel_file --> file path of excel file
#Return Values: pd.ExcelFile --> pandas array of excel file contents
############################################################################

    try:
        # explicit engine is optional but predictable
        return pd.ExcelFile(excel_file, engine="openpyxl")

    except Exception as err:
        messagebox.showerror(
            "Error",
            f"Failed to open Excel file:\n{err}",
            parent=parent
        )
        return None

def get_export_sheets(parent, xls):
############################################################################
#Function Name: get_export_sheets
#Description: Return all sheets beginning with Export_CSV_
#Parameters: parent --> parent ttk window
#            xls --> excel file
#Return Values: export_sheets --> array of names of all export sheets
############################################################################    

    export_sheets = [
        s for s in xls.sheet_names
        if s.startswith("Export_CSV_")
    ]

    if not export_sheets:
        messagebox.showwarning(
            "No Sheets Found",
            "No sheets starting with 'Export_CSV_' were found.",
            parent=parent
        )
        return None

    return export_sheets

def export_csv_sheets(
    parent,
    xls,
    export_sheets,
    output_folder,
    use_timestamp=True
):
############################################################################
#Function Name: export_csv_sheets
#Description: Export all matching sheets as CSV files.
#Parameters: parent --> parent ttk window
#            xls --> excel file
#            export_sheets --> array of names of all export sheets
#            output_folder --> folder path of selected output folder
#            use_timestamp --> used to attach timestamp to CSV file name
#Return Values: created_files --> array of names of all created CSV sheets
############################################################################  

    created_files = []

    try:
        timestamp = None

        if use_timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        for sheet in export_sheets:

            df = pd.read_excel(
                xls,
                sheet_name=sheet,
                dtype=str
            )

            # remove prefix for cleaner filenames
            prefix = sheet.replace("Export_CSV_", "")

            if use_timestamp:
                csv_file = output_folder / f"{prefix}_{timestamp}.csv"
            else:
                csv_file = output_folder / f"{prefix}.csv"

            df.to_csv(csv_file, index=False)

            created_files.append(csv_file)

    except Exception as err:
        messagebox.showerror(
            "Error",
            f"Failed during conversion:\n{err}",
            parent=parent
        )
        return None

    return created_files

def show_created_files(parent, created_files):
############################################################################
#Function Name: export_csv_sheets
#Description: Success window showing created CSVs.
#Parameters: parent --> parent ttk window
#            created_files --> array of names of all created CSV sheets
#Return Values: none
############################################################################  

    if not created_files:
        return

    win = tk.Toplevel(parent)
    win.title("CSV Export Complete")
    win.geometry("550x380")

    win.transient(parent)
    win.grab_set()

    ttk.Label(
        win,
        text="CSV Export Successful",
        font=("Segoe UI", 13, "bold")
    ).pack(pady=(15, 5))

    ttk.Label(
        win,
        text=f"{len(created_files)} file(s) created:"
    ).pack(pady=(0, 10))

    frame = ttk.Frame(win)
    frame.pack(
        fill="both",
        expand=True,
        padx=20,
        pady=10
    )

    scrollbar = ttk.Scrollbar(frame)
    scrollbar.pack(side="right", fill="y")

    text = tk.Text(
        frame,
        height=15,
        yscrollcommand=scrollbar.set,
        wrap="none"
    )
    text.pack(fill="both", expand=True)

    scrollbar.config(command=text.yview)

    for f in created_files:
        text.insert(
            "end",
            f"✓ {Path(f).name}\n"
        )

    text.config(state="disabled")

    ttk.Button(
        win,
        text="OK",
        command=win.destroy
    ).pack(pady=15)

    parent.wait_window(win)

#IMPORT MAIN GUI ENTRY
def run(parent):
############################################################################
#Function Name: run
#Description: Main function which runs the entire Excel to CSV export process
#             for the Import Main GUI. This entry point allows users to select
#             the Excel file and output folder for this process.
#Parameters: parent --> parent ttk window
#Return Values: none
############################################################################  

    excel_file = open_file_selection(parent)
    if not excel_file:
        return

    output_folder = open_output_folder(parent)
    if not output_folder:
        return

    xls = load_excel_file(parent, excel_file)
    if not xls:
        return

    export_sheets = get_export_sheets(parent, xls)
    if not export_sheets:
        return

    created_files = export_csv_sheets(
        parent,
        xls,
        export_sheets,
        output_folder,
        use_timestamp=True
    )

    show_created_files(
        parent,
        created_files
    )

#RUN-ALL AUTO ENTRY
def run_auto(parent, file_path, folder_path):
############################################################################
#Function Name: run_auto
#Description: Main function which runs the entire Excel to CSV export process
#             for the Run All script. This entry point has a preselected Excel
#             file and output folder for this process.
#Parameters: parent --> parent ttk window
#            file_path --> preselected file path for the Excel file
#            folder_path --> preselected folder for CSV outputs
#Return Values: none
############################################################################  

    excel_file = file_path
    if not excel_file:
        return

    if not folder_path:
        return

    output_folder = Path(folder_path)

    xls = load_excel_file(
        parent,
        excel_file
    )
    if not xls:
        return

    export_sheets = get_export_sheets(
        parent,
        xls
    )
    if not export_sheets:
        return

    created_files = export_csv_sheets(
        parent,
        xls,
        export_sheets,
        output_folder,
        use_timestamp=False
    )

    show_created_files(
        parent,
        created_files
    )