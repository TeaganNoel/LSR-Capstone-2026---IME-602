#Author: Mason Allen
#Description: Creates a GUI for operating all formatting and import scripts
#Created Date: 4/4/2026
#Last Updated Date: 4/4/2026

import ttkbootstrap as ttk
from ttkbootstrap.constants import *

from db.connection import get_connection

from import_scripts import ImportForm_GUI
from import_scripts import ImportCSV_GUI

from formatting_scripts import Dataq_MycronFormatting_GUI
from formatting_scripts import ExcelToCSV_GUI
from formatting_scripts import KestralFormatting_GUI
from formatting_scripts import dataq_parse_GUI
from formatting_scripts import aim_parse_segments_GUI

from GUI_scripts import IDNameKeyGenerator_GUI

connection = get_connection()

root = ttk.Window(themename="darkly")
root.title("Data Tools GUI")

#padding defaults
x1 = 5
x2 = 10
y1 = 8
y2 = 12

def open_import_form():
    ImportForm_GUI.run(root, connection, "lsr_testing_database")

def open_GUI_idnametool():
    IDNameKeyGenerator_GUI.run(root, connection, "lsr_testing_database")

def open_import_csv():
    ImportCSV_GUI.run(root, connection, "lsr_testing_database")

def open_format_exceltocsv():
    ExcelToCSV_GUI.run(root)

def open_format_kestral():
    KestralFormatting_GUI.run(root)

def open_format_dataqmychronmerge():
    Dataq_MycronFormatting_GUI.run(root)

def open_format_dataqparse():
    dataq_parse_GUI.run(root, connection, "lsr_testing_database")

def open_format_mychronparse():
    aim_parse_segments_GUI.run(root, connection, "lsr_testing_database")


ttk.Button(root, text="Form Import", command=open_import_form, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="CSV Import", command=open_import_csv, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="ID Tool", command=open_GUI_idnametool, bootstyle=SUCCESS).pack(pady=5)
ttk.Button(root, text="Format Kestral CSV", command=open_format_kestral, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="Extract Excel CSVs", command=open_format_exceltocsv, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="Format DataQ-MyChron CSV", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="Parse DataQ Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(pady=5)
ttk.Button(root, text="Parse MyChron Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(pady=5)

root.mainloop()