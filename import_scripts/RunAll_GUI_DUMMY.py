"""
############################################################################
Program Filename: RunAll_GUI_DUMMY.py
Author: Mason Allen
Created Date: 4/9/2026
Last Updated Date: 6/3/2026
Description: THIS IS A DUMMY SCRIPT WITH NO IMPORT OR FORMATTING FUNCTIONALITY. MADE FOR THE ENGR EXPO

             This script imports an entire field day's worth of data into the
             LSR testing database. All data must be formatted and labelled 
             correctly, as well as placed into a single data folder before 
             this script can be used. The script is made to be run from the 
             main import GUI.
############################################################################
"""

#import all libraries and scripts
import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
from pathlib import Path
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
import re

#from import_scripts import ImportCSVSpecific_GUI
from import_scripts import ImportForm_GUI_DUMMY

#from formatting_scripts import Dataq_MychronFormatting_GUI
#from formatting_scripts import ExcelToCSV_GUI
#from formatting_scripts import KestrelFormatting_GUI
#from formatting_scripts import datalog_sync_master_GUI

class UserAbort(Exception):
############################################################################
#Class Name: UserAbort
#Description: Raised when the user intentionally cancels part of the
#             Run All workflow. Used to stop processing cleanly and
#             return control to the main GUI without treating it as
#             an application error.
############################################################################
    pass

class RunContext:
############################################################################
#Class Name: RunContext
#Description: Tracks state across the Run All import workflow,
#             including which database tables were successfully populated.
#             Used for end-of-run or abort summaries.
############################################################################
    def __init__(self):
        self.imported_tables = set()

def open_folder_selection(parent):
############################################################################
#Function Name: open_folder_selection
#Description: opens a file directory prompt to allow user to select a data
#             folder path. 
#Parameters: parent --> parent ttk window
#Return Values: folder_path --> data folder path
############################################################################

    try:
        folder_path = fd.askdirectory(
            parent=parent,
            title="Select Data Folder"
        )
        
        # If no folder selected
        if not folder_path:
            print("User cancelled folder selection")
            raise UserAbort("Folder selection cancelled")
    
    except UserAbort:
        raise  # re-raise cleanly

    except Exception as e:
        print(f"Error selecting folder: {e}")
        return None

    return folder_path

def run_form_loop(parent, connection, ctx):
############################################################################
#Function Name: run_form_loop
#Description: Used primarily for importing low density data like an additional
#             rider, a new locations, etc... The function opens a db form entry 
#             window and imports data to the selected table. Form entry can be 
#             repeated until user is satisfied. 
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            ctx --> run context for tracking imported tables
#Return Values: none
############################################################################

    while True:

        success, imported_tbl = ImportForm_GUI_DUMMY.run(
            parent,
            connection,
            "lsr_testing_database"
        )

        if success:
            ctx.imported_tables.add(imported_tbl)

        again = messagebox.askyesno(
            "Continue?",
            "Would you like to enter another form?",
            parent=parent
        )

        if not again:
            break

def run_form_loop_prototype(parent, connection, table, ctx):
############################################################################
#Function Name: run_form_loop_prototype
#Description: Used for importing new prototypes into the db. The function opens 
#             a db form entry window and imports data to the prototype table. 
#             Form entry can be repeated until user is satisfied. 
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            table --> prototype table
#            ctx --> run context for tracking imported tables
#Return Values: none
############################################################################

    while True:

        success = ImportForm_GUI_DUMMY.run_auto(
            parent,
            connection,
            "lsr_testing_database",
            table
        )

        if success:
            ctx.imported_tables.add("prototype")

        again = messagebox.askyesno(
            "Continue?",
            "Would you like to enter another prototype?",
            parent=parent
        )

        if not again:
            break

def run_form_loop_condition(parent, connection, table, ctx):
############################################################################
#Function Name: run_form_loop_condition
#Description: Used for importing new conditions into the db. The function opens 
#             a db form entry window and imports data to the condition table. 
#             Form entry can be repeated until user is satisfied. 
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            table --> condition table
#            ctx --> run context for tracking imported tables
#Return Values: none
############################################################################

    while True:

        success = ImportForm_GUI_DUMMY.run_auto(
            parent,
            connection,
            "lsr_testing_database",
            table
        )

        if success:
            ctx.imported_tables.add("condition_tbl")

        again = messagebox.askyesno(
            "Continue?",
            "Would you like to enter another condition?",
            parent=parent
        )

        if not again:
            break

def ask_import_setup(parent):
############################################################################
#Function Name: ask_import_setup
#Description: opens a window to ask user what type of import they would like
#             to run, a single XLSX file, or a multi-CSV import. At time of
#             documentation, single XLSX imports are not active
#Parameters: parent --> parent ttk window
#Return Values: value --> user choice var
############################################################################

    result = {"value": None}

    win = tk.Toplevel(parent)
    win.title("Import Type Selection")

    tk.Label(win, text="Are you trying to import a single XLSX file, or multiple CSVs?").pack(padx=20, pady=10)

    def choose_xlsx():
        result["value"] = "x"
        win.destroy()

    def choose_csv():
        result["value"] = "c"
        win.destroy()

    tk.Button(win, text="XLSX", command=choose_xlsx).pack(fill="x", padx=20, pady=5)
    tk.Button(win, text="CSVs", command=choose_csv).pack(fill="x", padx=20, pady=5)

    win.transient(parent)

    try:
        win.grab_set()
        parent.wait_window(win)

    finally:
        try:
            if win.winfo_exists():
                win.grab_release()
        except tk.TclError:
            pass

    return result["value"]

def ask_vehicle(parent):
############################################################################
#Function Name: ask_vehicle
#Description: opens a window to ask user what type of vehicle was used in the
#             field day test. Result affects which datalogger import is run
#Parameters: parent --> parent ttk window
#Return Values: value --> user choice var
############################################################################    
    
    result = {"value": None}

    win = tk.Toplevel(parent)
    win.title("Vehicle Selection")

    tk.Label(win, text="Which vehicle was used?").pack(padx=20, pady=10)

    def choose_derbi():
        result["value"] = "d"
        win.destroy()

    def choose_aprilia():
        result["value"] = "a"
        win.destroy()
    
    def choose_none():
        result["value"] = "n"
        win.destroy()

    tk.Button(win, text="Derbi", command=choose_derbi).pack(fill="x", padx=20, pady=5)
    tk.Button(win, text="Aprilia", command=choose_aprilia).pack(fill="x", padx=20, pady=5)
    tk.Button(win, text="Neither", command=choose_none).pack(fill="x", padx=20, pady=5)

    win.transient(parent)

    try:
        win.grab_set()
        parent.wait_window(win)

    finally:
        try:
            if win.winfo_exists():
                win.grab_release()
        except tk.TclError:
            pass

    return result["value"]

def csv_import(parent, connection, db, file, table, ctx):
############################################################################
#Function Name: csv_import
#Description: Runs ImportCSVSpecific_GUI to import a CSV file into the db.
#             Tracks the import success and updates the imported table list.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            db --> "lsr_testing_database"
#            file --> file path of CSV
#            table --> specific db table name
#            ctx --> run context for tracking imported tables
#Return Values: success --> tracks whether import occured or not
############################################################################
    
    success = True

    if success:
        ctx.imported_tables.add(table)

    else:
        messagebox.showinfo("Import Failure", 
                            f"{file} could not be imported into the {table} table.", 
                            parent=parent)
    
    return success

def show_summary(parent, imported_tables, aborted=False):
############################################################################
#Function Name: show_summary
#Description: once the Run All script ends, or if User Abort occurs, this
#             script displays a window with a list of all imported tables
#             from this Run All session.
#Parameters: parent --> parent ttk window
#            imported_tables --> set with all imported table names
#            aborted --> indicates whether User Abort occured, default is false
#Return Values: none
############################################################################
    
    win = ttk.Toplevel(parent)
    win.title("Import Summary")
    win.geometry("400x300")

    frame = ttk.Frame(win, padding=15)
    frame.pack(fill=BOTH, expand=True)

    title = "Import Aborted" if aborted else "Import Complete"

    ttk.Label(
        frame,
        text=title,
        font=("Segoe UI", 14, "bold"),
        bootstyle=DANGER if aborted else SUCCESS
    ).pack(pady=(0, 10))

    if imported_tables:
        ttk.Label(frame, text="Tables successfully imported:").pack(anchor="w")

        listbox = tk.Listbox(frame)
        listbox.pack(fill=BOTH, expand=True, pady=10)

        for tbl in sorted(imported_tables):
            listbox.insert(tk.END, tbl)

    else:
        ttk.Label(frame, text="No tables were imported.").pack()

    ttk.Button(
        frame,
        text="Close",
        bootstyle=PRIMARY,
        command=win.destroy
    ).pack(pady=10)

    win.transient(parent)

    try:
        win.grab_set()
        parent.wait_window(win)

    finally:
        try:
            if win.winfo_exists():
                win.grab_release()
        except tk.TclError:
            pass

def extract_test_id_kestrel(filename):
############################################################################
#Function Name: extract_test_id_kestrel
#Description: extracts the test ID from the file name of a kestrel file
#Parameters: filename --> file name containing ID
#Return Values: match.group(1) --> testID
#               None --> None if no testID was found
############################################################################    
    
    match = re.search(r"kestrel_original_(\w+)", filename.stem)
    return match.group(1) if match else None

def extract_test_id_dataq_mychron(filename):
############################################################################
#Function Name: extract_test_id_dataq_mychron
#Description: extracts the test ID from the file name of a dataq_mychron file
#Parameters: filename --> file name containing ID
#Return Values: match.group(1) --> testID
#               None --> None if no testID was found
############################################################################       
    
    match = re.search(r"dq_my_or_(\w+)", filename.stem)
    return match.group(1) if match else None

#MAIN IMPORT GUI ENTRY POINT
def run(parent, connection, database_name):
############################################################################
#Function Name: run
#Description: Entry point from import main gui. Runs all functions for data
#             formatting and import, as well as user decision-making. The 
#             order of formatting and imports is highly specfic because of 
#             the way relations in the db are set up. Refer to documentation
#             of both the Run All script and the database layout before making
#             any changes to this function.
#Parameters: parent --> parent ttk window
#            connection --> MySQL db connection for queries and imports
#            database_name --> "lsr_testing_database"
#Return Values: none
############################################################################
    
    #initialize imported table tracking
    ctx = RunContext()
    
    #error wrapper
    try:
        #ask user to select a data folder
        proceed = messagebox.askokcancel(
            "Select Data Folder",
            "Please select the appropriate data folder in the next window",
            parent=parent
        )

        #abort if user selects "cancel"
        if not proceed:
            raise UserAbort("User cancelled")

        #open data folder selection window
        selected = open_folder_selection(parent)

        #abort if user does not select a data folder
        if not selected:
            raise UserAbort(...)
        
        #turn folder path into a Path format
        folder_path = Path(selected)


        #If there is form data to enter, prompt user to import form data until they are satisfied
        answer = messagebox.askyesno(
            "Import Form Entry?",
            "Would you like to import a form entry?",
            parent=parent
        )

        #open import form if user selects "yes"
        if answer:
            run_form_loop(parent, connection, ctx)

        #define file path for Excel datasheet based on labelling convention
        file_path = folder_path / "excel_datasheet.xlsx"

        #turn datasheet Excel into CSVs
        #ExcelToCSV_GUI.run_auto(
        #    parent,
        #    file_path,
        #    folder_path
        #)

        #Import configuration tbl
        file_path = folder_path / "configuration.csv"
        table_name = "configuration"
        csv_import(parent, connection, database_name, file_path, table_name, ctx)

        #Import test tbl
        file_path = folder_path / "test.csv"
        table_name = "test"
        csv_import(parent, connection, database_name, file_path, table_name, ctx)
        
        #Import external media tbl
        file_path = folder_path / "media.csv"
        table_name = "external_media"
        csv_import(parent, connection, database_name, file_path, table_name, ctx)
        
        #Ask user if prototypes were tested during the field day
        answer = messagebox.askyesno(
            "Prototypes?",
            "Were any prototypes tested during this field day?",
            parent=parent
        )

        #if user selects "yes", ask user if the prototype already exists in the db
        if answer:
            answer = messagebox.askyesno(
                "Prototypes?",
                "Does this prototype(s) already exist in the database?",
                parent=parent
            )
            
            #if user selects "no", open form import for prototype tbl
            if not answer:
                
                #Import prototypes to prototype tbl
                table_name = "prototype"
                run_form_loop_prototype(parent, connection, table_name, ctx)
                
                #Import rel_prototype_test tbl
                file_path = folder_path / "rel_prototype_test.csv"
                table_name = "rel_prototype_test"
                csv_import(parent, connection, database_name, file_path, table_name, ctx)

        #Ask if specific conditions were tested during the field day
        answer = messagebox.askyesno(
            "Conditions?",
            "Were specific conditions tested during this field day?",
            parent=parent
        )

        #if user selects "yes", ask user if the condition already exists in the db
        if answer:
            
            answer = messagebox.askyesno(
                "Conditions?",
                "Does this condition(s) already exist in the database?",
                parent=parent
            )

            #if user selects "no", open form import for condition tbl
            if not answer:
        
                #Import conditions to condition_tbl tbl       
                table_name = "condition_tbl"
                run_form_loop_condition(parent, connection, table_name, ctx)
                
                #Import rel_condition_test tbl
                file_path = folder_path / "rel_condition_test.csv"
                table_name = "rel_condition_test"
                csv_import(parent, connection, database_name, file_path, table_name, ctx)

        #Import rel_device_test tbl
        file_path = folder_path / "rel_device_test.csv"
        table_name = "rel_data_collection_device_test"
        csv_import(parent, connection, database_name, file_path, table_name, ctx)

        #Import rel_media_test tbl
        file_path = folder_path / "rel_media_test.csv"
        table_name = "rel_external_media_test"
        csv_import(parent, connection, database_name, file_path, table_name, ctx)
        
        #ask user which vehicle was used during field day testing
        answer = ask_vehicle(parent)

        #if user selects derbi...
        if answer == "d":

            #compile list of datalogger files using labelling convention
            data_files = list(folder_path.glob("dq_my_or_*.csv"))

            if not data_files:
                #warn user if theres no files
                messagebox.showwarning("No Files", "No DataQ-Mychron files found.", parent=parent)
            
            else:
                #import files one by one
                for file_path in data_files:

                    #extract testID from file name
                    testID = extract_test_id_dataq_mychron(file_path)

                    #if testID is not located, skip import
                    if not testID:
                        print(f"Skipping file (no testID found): {file_path}")
                        continue

                    print(f"Processing Dataq-Mychron file: {file_path} with testID={testID}")

                    #format datalogger file for db CSV import
                    output_file = True
                    #output_file = Dataq_MychronFormatting_GUI.run_auto_noform(
                    #    file_path,
                    #    folder_path,
                    #    testID,
                    #    parent
                    #)

                    if output_file:

                        #import dataq_mychron3_data tbl
                        table_name = "dataq_mychron3_data"

                        success = csv_import(
                            parent,
                            connection,
                            database_name,
                            output_file,
                            table_name,
                            ctx
                        )

                        #track success specific to testID
                        if success:
                            ctx.imported_tables.add(f"{table_name} ({testID})")

        #if user selects "Aprilia"... (unfinished section of script. Update when MaxxECU tbl is active in db)
        elif answer == "a":
            print("Will run Maxx ECU merge")
            print("Then will run Maxx ECU import")

        #if no vehicle is selected, skip datalogger import
        else:
            print("No datalogger data will be imported")

        #Ask if Kestrel was used during the field day
        answer = messagebox.askyesno(
            "Kestrel?",
            "Was the Kestrel handheld weather station used during this field day?",
            parent=parent
        )

        #if user selects "yes"...
        if answer:
            
            #compile list of Kestrel files using labelling convention
            kestrel_files = list(folder_path.glob("kestrel_original_*.csv"))

            if not kestrel_files:
                #warn user if theres no files
                messagebox.showwarning("No Files", "No Kestrel files found.", parent=parent)
            
            else:
                #iterate through Kestrel files
                for file_path in kestrel_files:

                    #extract testID from file names
                    testID = extract_test_id_kestrel(file_path)
                    
                    #if testID is not located, skip import
                    if not testID:
                        print(f"Skipping file (no testID found): {file_path}")
                        continue

                    print(f"Processing Kestrel file: {file_path} with testID={testID}")

                    #format Kestrel file for db CSV import
                    output_file = True
                    #output_file = KestrelFormatting_GUI.run_auto_noform(
                    #    file_path,
                    #    folder_path,
                    #    testID,
                    #    parent
                    #)

                    if output_file:

                        #import Kestrel_data tbl
                        table_name = "kestrel_data"

                        success = csv_import(
                            parent,
                            connection,
                            database_name,
                            output_file,
                            table_name,
                            ctx
                        )
                        
                        #track success specific to testID
                        if success:
                            ctx.imported_tables.add(f"{table_name} ({testID})")

        #once all imports are complete, display all tables that were successfully updated during this
        #Run All session
        show_summary(parent, ctx.imported_tables, aborted=False)
    
    #if user aborts at any time, show the import summary
    except UserAbort:
        print("User aborted")

        show_summary(parent, ctx.imported_tables, aborted=True)

    #if another error occurs, show the error in a popup
    except Exception as e:
        messagebox.showerror("Error", str(e), parent=parent)