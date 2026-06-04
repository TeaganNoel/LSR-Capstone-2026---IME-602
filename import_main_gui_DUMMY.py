############################################################################
#Program Filename: import_main_gui_DUMMY.py
#Author: Mason Allen
#Created Date: 4/4/2026
#Last Updated Date: 6/3/2026
#Description: THIS IS A DUMMY SCRIPT WITH NO IMPORT OR FORMATTING FUNCTIONALITY. MADE FOR THE ENGR EXPO
# 
#             Creates a GUI for operating all formatting and import scripts. 
#             Provides a user friendly interface for storing data in the LSR
#             testing database.
############################################################################

#import all needed libraries and scripts
import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from PIL import Image, ImageTk
from pathlib import Path

from db.connection import get_connection

from import_scripts import ImportForm_GUI_DUMMY
from import_scripts import ImportCSV_GUI_DUMMY
from import_scripts import RunAll_GUI_DUMMY

from formatting_scripts import Dataq_MychronFormatting_GUI
from formatting_scripts import ExcelToCSV_GUI
from formatting_scripts import KestrelFormatting_GUI
from formatting_scripts import dataq_parse_GUI
from formatting_scripts import aim_parse_segments_GUI
from formatting_scripts import datalog_sync_master_GUI

from info_scripts import IDNameKeyGenerator_GUI
from info_scripts import LastIDGenerator_GUI

#start with no connection to db
connection = None

#start tkinter window
root = ttk.Window(themename="darkly")
root.title("Data Tools GUI")
root.geometry("1000x700")

#define padding defaults
x1 = 5
x2 = 10
y1 = 8
y2 = 12

def get_live_connection():
############################################################################
#Function Name: get_live_connection
#Description: establishes connection with db as long as script is not currently connected
#Parameters: none
#Return Values: connection
############################################################################

    global connection

    try:
        if connection is None or not connection.is_connected():
            connection = get_connection()

    except Exception:
        connection = get_connection()

    return connection

def on_close():
############################################################################
#Function Name: on_close
#Description: close connection and destroy window upose exiting GUI
#Parameters: none
#Return Values: none
############################################################################

    global connection
    try:
        if connection is not None and connection.is_connected():
            connection.close()
    except Exception as e:
        print(e)

    root.destroy()

#define button commands (references specific scripts)
#####################################################
def open_import_form():
    ImportForm_GUI_DUMMY.run(root, get_live_connection(), "lsr_testing_database")

def open_info_idnametool():
    IDNameKeyGenerator_GUI.run(root, get_live_connection(), "lsr_testing_database")

def open_import_csv():
    ImportCSV_GUI_DUMMY.run(root, get_live_connection(), "lsr_testing_database")

def open_format_exceltocsv():
    ExcelToCSV_GUI.run(root)

def open_format_kestrel():
    KestrelFormatting_GUI.run(root)

def open_format_dataqmychronformat():
    Dataq_MychronFormatting_GUI.run(root)

def open_format_dataqparse():
    dataq_parse_GUI.run(root)

def open_format_mychronparse():
    aim_parse_segments_GUI.run(root)

def open_format_dataqmychronmerge():
    datalog_sync_master_GUI.run(root)

def open_runall():
    RunAll_GUI_DUMMY.run(root, get_live_connection(), "lsr_testing_database")
    return

def open_info_lastID():
    LastIDGenerator_GUI.run(root, get_live_connection(), "lsr_testing_database")
    return
#########################################################

#create title frame
f1 = ttk.Frame(root)
f1.pack(fill="x")

#image import
BASE_DIR = Path(__file__).resolve().parent

try:
    img_path = BASE_DIR / "graphics" / "LSR_logo-final-tagline.png"

    img = Image.open(img_path)
    img = img.resize((200,80))
    logo = ImageTk.PhotoImage(img)

    ttk.Label(
        f1,
        image=logo
    ).grid(row=0,column=0,padx=x1,pady=y1,sticky="W")

except Exception as e:
    print(f"Warning: logo failed to load: {e}")

    ttk.Label(
        f1,
        text="LSR Data Tools",
        font=("Verdana",16)
    ).grid(row=0,column=0,padx=x1,pady=y1,sticky="W")

#add GUI title
ttk.Label(f1, text="Import Files to Database (sample version)", font=("Verdana",18), bootstyle=LIGHT).grid(row=0,column=1,padx=x1,pady=y1,sticky="W")
ttk.Separator(root, orient='horizontal', bootstyle=LIGHT).pack(fill='x', padx=10, pady=5)

#create RUN ALL label frame
lf1 = ttk.Labelframe(root, text = "Full Field Day Pre-processing", bootstyle=LIGHT)
lf1.pack(fill='both', expand=True, padx = x1, pady = y1)

#add "Run All" button + description
ttk.Button(lf1, text = "Run All", command=open_runall, bootstyle = SUCCESS).pack(fill="x",side='left',expand=True, padx = x1, pady = y1,anchor="n")
ttk.Label(lf1, text="The Run All tool is used to import all data from a SINGLE field day. \n" \
                    "Before using this tool, please ensure that all datafiles are labelled \n"
                    "and formatted for the Run All tool, and placed in a single data folder.\n" \
                    "See Run All SOP for in-depth usage instructions.", bootstyle=LIGHT).pack(fill="x",side='right',expand=True, padx = x1, pady = y1,anchor="n")

#create middle frame
f2 = ttk.Frame(root)
f2.pack(fill="both", expand=True)

#create lower-left-middle frame
f3 = ttk.Frame(f2)
f3.pack(fill="both",side='left',expand=True)

#create lower-right-middle frame
f4 = ttk.Frame(f2)
f4.pack(fill="both",side='right',expand=True)

#add Format and Merge label frame
lf2 = ttk.Labelframe(f3, text = "Format & Merge", bootstyle="light")
lf2.pack(fill='both', expand=True, padx = x1, pady = y1)

#add Import label frame
lf3 = ttk.Labelframe(f4, text = "Import", bootstyle="light")
lf3.pack(fill='both', expand=True, padx = x1, pady = y1)

#add Info label frame
lf4 = ttk.Labelframe(f4, text = "Information", bootstyle="light")
lf4.pack(fill='both', expand=True, padx = x1, pady = y1)

#add Formatting buttons
ttk.Button(lf2, text="Extract CSVs from Excel", command=open_format_exceltocsv, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Format Kestrel CSV", command=open_format_kestrel, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Format DataQ-MyChron CSV", command=open_format_dataqmychronformat, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Format MaxxECU File", bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1) #NULL
ttk.Button(lf2, text="Merge DataQ & MyChron Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)

#add Import buttons
ttk.Button(lf3, text="Form Import", command=open_import_form, bootstyle=INFO).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf3, text="CSV Import", command=open_import_csv, bootstyle=INFO).pack(fill='x', padx = x1, pady = y1)

#add Info buttons
ttk.Button(lf4, text="View ID-Name Key", command=open_info_idnametool, bootstyle=SECONDARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf4, text="View Last/Next ID", command=open_info_lastID, bootstyle=SECONDARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf4, text="View Database Schema", bootstyle=DARK).pack(fill='x', padx = x1, pady = y1)

#UNUSED menu button 
##########################################################################
#mb1 = ttk.Menubutton(lf2, text="Format Datalogger File", bootstyle=PRIMARY)
#mb1.pack(fill='x', padx = x1, pady = y1)
#m1 = ttk.Menu(mb1, tearoff=0)
#mb1["menu"] = m1
#m1.add_command(label="Parse DataQ", command=open_format_dataqparse)
#m1.add_command(label="Parse MyChron", command=open_format_mychronparse)
#m1.add_command(label="Merge DataQ & MyChron Data", command=open_format_dataqmychronmerge)
#m1.add_command(label="Format MaxxECU File", command=open_format_dataqmychronmerge) #CHANGE COMMAND
##########################################################################

#define exit protocol
root.protocol("WM_DELETE_WINDOW", on_close)

#initialize tkinter mainloop
root.mainloop()