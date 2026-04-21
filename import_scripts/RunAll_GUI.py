#Author: Mason Allen
#Description: Made for connection with Main GUI
#Created Date: 4/9/2026
#Last Updated Date: 4/9/2026

import tkinter as tk
from tkinter import filedialog as fd
from tkinter import messagebox
from pathlib import Path
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
import re

from import_scripts import ImportCSVSpecific_GUI
from import_scripts import ImportForm_GUI
from import_scripts import ImportCSV_GUI

from formatting_scripts import Dataq_MychronFormatting_GUI
from formatting_scripts import ExcelToCSV_GUI
from formatting_scripts import KestrelFormatting_GUI
from formatting_scripts import dataq_parse_GUI
from formatting_scripts import aim_parse_segments_GUI
from formatting_scripts import datalog_sync_master_GUI

from info_scripts import IDNameKeyGenerator_GUI



class UserAbort(Exception):
    pass

class RunContext:
    def __init__(self):
        self.imported_tables = set()

def open_folder_selection(parent):
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
        #messagebox.showinfo("Import Aborted", "Import aborted, press ok to return to main menu", parent=parent)
        raise  # re-raise cleanly

    except Exception as e:
        print(f"Error selecting folder: {e}")
        return None

    return folder_path

def run_form_loop(parent, connection, ctx):
    success, imported_tbl = ImportForm_GUI.run(parent, connection, "lsr_testing_database")
    if success:
        ctx.imported_tables.add(imported_tbl)

    again = messagebox.askyesno(
        "Continue?",
        "Would you like to enter another form?",
        parent=parent
    )
    if again:
        run_form_loop(parent, connection, ctx)

def run_form_loop_prototype(parent, connection, table, ctx):
    success = ImportForm_GUI.run_auto(parent, connection, "lsr_testing_database", table)
    if success:
        ctx.imported_tables.add("prototype")

    again = messagebox.askyesno(
        "Continue?",
        "Would you like to enter another prototype?",
        parent=parent
    )
    if again:
        run_form_loop_prototype(parent, connection, table, ctx)

def run_form_loop_condition(parent, connection, table, ctx):
    success = ImportForm_GUI.run_auto(parent, connection, "lsr_testing_database", table)
    if success:
        ctx.imported_tables.add("condition_tbl")

    again = messagebox.askyesno(
        "Continue?",
        "Would you like to enter another condition?",
        parent=parent
    )
    if again:
        run_form_loop_condition(parent, connection, table, ctx)

def ask_import_setup(parent):
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
    win.grab_set()
    parent.wait_window(win)

    return result["value"]

def ask_vehicle(parent):
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
    win.grab_set()
    parent.wait_window(win)

    return result["value"]

def csv_import(parent, connection, db, file, table, ctx):
    success = ImportCSVSpecific_GUI.run(parent, connection, db, file, table)

    if success:
        ctx.imported_tables.add(table)
    else:
        messagebox.showinfo("Import Failure", f"{file} could not be imported into the {table} table.", parent=parent)

def show_summary(parent, imported_tables, aborted=False):
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
    win.grab_set()
    parent.wait_window(win)

def extract_test_id_kestrel(filename):
    match = re.search(r"kestrel_original_(\w+)", filename.stem)
    return match.group(1) if match else None

def extract_test_id_dataq_mychron(filename):
    match = re.search(r"dq_my_or_(\w+)", filename.stem)
    return match.group(1) if match else None

# 🔹 MAIN ENTRY POINT
def run(parent, connection, database_name):
    
    ctx = RunContext()

    try:

        #messagebox.showinfo("Select Data Folder", "Please select the appropriate data folder in the next window", parent=parent)

        #Choose data folder. Ensure ALL data files are in this folder
        #folder_path = open_folder_selection(parent)


        #####################
        proceed = messagebox.askokcancel(
            "Select Data Folder",
            "Please select the appropriate data folder in the next window",
            parent=parent
        )

        if not proceed:
            #messagebox.showinfo("Import Aborted", "Import aborted, press ok to return to main menu", parent=parent)
            raise UserAbort("User cancelled")

        folder_path = Path(open_folder_selection(parent))

        ##################
        

        #If there is form data to enter, prompt user to import form data until they are satisfied
        answer = messagebox.askyesno(
            "Import Form Entry?",
            "Would you to import a form entry?",
            parent=parent
        )
        if answer:
            run_form_loop(parent, connection, ctx)

        #messagebox.showinfo("Select Excel Data Sheet", "Please select the Excel data sheet from the field day in the next window", parent=parent)
        
        #proceed = messagebox.askokcancel(
        #    "Select Excel Data Sheet",
        #    "Please select the Excel data sheet from the field day in the next window",
        #    parent=parent
        #)

        #if not proceed:
        ##    messagebox.showinfo("Import Aborted", "Import aborted, press ok to return to main menu", parent=parent)
        #   raise UserAbort("User cancelled")

        file_path = folder_path / "excel_datasheet.xlsx"

        #Turn Datasheet Excel into CSVs
        ExcelToCSV_GUI.run_auto(
            parent,
            file_path,
            folder_path
        )

        #Import configuration tbl
        file_path = folder_path / "configuration.csv"
        table_name = "configuration"
        #ImportCSVSpecific_GUI_old.run(parent, connection, database_name, file_path, table_name)
        csv_import(parent, connection, database_name, file_path, table_name, ctx)

        #Import test tbl
        file_path = folder_path / "test.csv"
        table_name = "test"
        #ImportCSVSpecific_GUI_old.run(parent, connection, database_name, file_path, table_name)
        csv_import(parent, connection, database_name, file_path, table_name, ctx)
        
        #Import external media tbl
        file_path = folder_path / "media.csv"
        table_name = "external_media"
        #ImportCSVSpecific_GUI_old.run(parent, connection, database_name, file_path, table_name)
        csv_import(parent, connection, database_name, file_path, table_name, ctx)
        
        #Ask if prototypes were tested during the field day
        answer = messagebox.askyesno(
            "Prototypes?",
            "Were any prototypes tested during this field day?",
            parent=parent
        )
        if answer:
            
            answer = messagebox.askyesno(
                "Prototypes?",
                "Does this prototype(s) already exist in the database?",
                parent=parent
            )
            if not answer:
                
                #Import prototypes
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
        if answer:
            
            answer = messagebox.askyesno(
                "Conditions?",
                "Does this condition(s) already exist in the database?",
                parent=parent
            )
            if not answer:
        
                #Import conditions        
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
        
        answer = ask_vehicle(parent)
        if answer == "d":
            
            answer = ask_import_setup(parent)
            if answer == "x":
                
                messagebox.showwarning("Under Construction", "Single Excel import is not functional yet. Skipping datalogger entry...", parent=parent)
                return
                #Merge DataQ & MyChron files
                datalog_sync_master_GUI.run_auto(parent, folder_path)

                #Extract individual test csvs from excel
                file_path = folder_path / "dataq_mychron_merge.xlsx"
                ExcelToCSV_GUI.run_auto(
                    parent,
                    file_path,
                    folder_path
                )

            if answer == "c":
                data_files = list(folder_path.glob("dq_my_or_*.csv"))

                if not data_files:
                    messagebox.showwarning("No Files", "No DataQ-Mychron files found.", parent=parent)
                else:

                    for file_path in data_files:
                        testID = extract_test_id_dataq_mychron(file_path)

                        if not testID:
                            print(f"Skipping file (no testID found): {file_path}")
                            continue

                        print(f"Processing Dataq-Mychron file: {file_path} with testID={testID}")

                        output_file = Dataq_MychronFormatting_GUI.run_auto_noform(
                            file_path,
                            folder_path,
                            testID,
                            parent
                        )

                        if output_file:
                            table_name = "dataq_mychron3_data"

                            success = csv_import(
                                parent,
                                connection,
                                database_name,
                                output_file,
                                table_name,
                                ctx
                            )

                            if success:
                                ctx.imported_tables.append(f"{table_name} ({testID})")


                #OLD
                #Import condition tbl
                #file_path = folder_path / "dataq_mychron3_data.csv"
                #table_name = "dataq_mychron3_data"
                #csv_import(parent, connection, database_name, file_path, table_name, ctx)


        elif answer == "a":
            print("Will run Maxx ECU merge")
            print("Then will run Maxx ECU import")

        else:
            print("No datalogger data will be imported")

        #Ask if Kestrel during the field day
        answer = messagebox.askyesno(
            "Kestrel?",
            "Was the Kestrel handheld weather station used during this field day?",
            parent=parent
        )

        #OLD Kestrel Import logic (single file import)
        ####################
        #if answer:
        #    #Format Kestral CSV
        #    file_path = folder_path / "kestrel_original.csv"
        #    KestrelFormatting_GUI.run_auto(parent, file_path, folder_path)
            
            #Import Kestral tbl
        #    file_path = folder_path / "kestrel.csv"
        #    table_name = "kestrel_data"
            #ImportCSVSpecific_GUI_old.run(parent, connection, database_name, file_path, table_name)
        #    csv_import(parent, connection, database_name, file_path, table_name, ctx)
        ###############################################

        #NEW Kestrel Import Logic (multiple)
        if answer:
            kestrel_files = list(folder_path.glob("kestrel_original_*.csv"))

            if not kestrel_files:
                messagebox.showwarning("No Files", "No Kestrel files found.", parent=parent)
            else:

                for file_path in kestrel_files:
                    testID = extract_test_id_kestrel(file_path)

                    if not testID:
                        print(f"Skipping file (no testID found): {file_path}")
                        continue

                    print(f"Processing Kestrel file: {file_path} with testID={testID}")

                    output_file = KestrelFormatting_GUI.run_auto_noform(
                        file_path,
                        folder_path,
                        testID,
                        parent
                    )

                    if output_file:
                        table_name = "kestrel_data"

                        success = csv_import(
                            parent,
                            connection,
                            database_name,
                            output_file,
                            table_name,
                            ctx
                        )

                        if success:
                            ctx.imported_tables.append(f"{table_name} ({testID})")

        show_summary(parent, ctx.imported_tables, aborted=False)
        #messagebox.showinfo("Import Complete", "Import complete, press ok to return to main menu", parent=parent)

    except UserAbort:
        print("User aborted")

        show_summary(parent, ctx.imported_tables, aborted=True)

    except Exception as e:
        messagebox.showerror("Error", str(e), parent=parent)

    