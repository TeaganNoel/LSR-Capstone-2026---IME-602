#Author: Mason Allen
#Description: Creates a GUI for operating all formatting and import scripts
#Created Date: 4/4/2026
#Last Updated Date: 4/9/2026

import ttkbootstrap as ttk
from ttkbootstrap.constants import *
from tkinter import PhotoImage
from PIL import Image, ImageTk

from db.connection import get_connection

from import_scripts import ImportForm_GUI
from import_scripts import ImportCSV_GUI
from import_scripts import RunAll_GUI

from formatting_scripts import Dataq_MychronFormatting_GUI
from formatting_scripts import ExcelToCSV_GUI
from formatting_scripts import KestrelFormatting_GUI
from formatting_scripts import dataq_parse_GUI
from formatting_scripts import aim_parse_segments_GUI
from formatting_scripts import datalog_sync_master_GUI

from info_scripts import IDNameKeyGenerator_GUI

connection = get_connection()


root = ttk.Window(themename="darkly")
root.title("Data Tools GUI")
root.geometry("1000x700")

#padding defaults
x1 = 5
x2 = 10
y1 = 8
y2 = 12

# Configure expansion
for i in range(3):
    root.columnconfigure(i, weight=1)

for i in range(6):
    root.rowconfigure(i, weight=1)

def open_import_form():
    ImportForm_GUI.run(root, connection, "lsr_testing_database")

def open_GUI_idnametool():
    IDNameKeyGenerator_GUI.run(root, connection, "lsr_testing_database")

def open_import_csv():
    ImportCSV_GUI.run(root, connection, "lsr_testing_database")

def open_format_exceltocsv():
    ExcelToCSV_GUI.run(root)

def open_format_kestral():
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
    RunAll_GUI.run(root, connection, "lsr_testing_database")
    return

#top frame
f1 = ttk.Frame(root)
f1.pack(fill="x")

img = Image.open("graphics/LSR_logo-final-tagline.png")

# Resize (adjust numbers as needed)
img = img.resize((200, 80))
logo = ImageTk.PhotoImage(img)

ttk.Label(f1, image=logo).grid(row=0,column=0,padx=x1,pady=y1,sticky="W")

ttk.Label(f1, text="Import Files to Database", font=("Verdana",18), bootstyle=LIGHT).grid(row=0,column=1,padx=x1,pady=y1,sticky="W")
#.grid(row=0,column=1,padx=x1,pady=y1,sticky="nsew")
#.pack(padx = x1, pady = y1)

ttk.Separator(root, orient='horizontal', bootstyle=LIGHT).pack(fill='x', padx=10, pady=5)

#RUN ALL label frame
lf1 = ttk.Labelframe(root, text = "Full Field Day Pre-processing", bootstyle=LIGHT)
lf1.pack(fill='both', expand=True, padx = x1, pady = y1)

ttk.Button(lf1, text = "Run All", command=open_runall, bootstyle = SUCCESS).pack(fill="x",side='left',expand=True, padx = x1, pady = y1,anchor="n")
ttk.Label(lf1, text="Description of runall", bootstyle=LIGHT).pack(fill="x",side='right',expand=True, padx = x1, pady = y1,anchor="n")


#middle frame
f2 = ttk.Frame(root)
f2.pack(fill="both", expand=True)

#lower-left-middle frame
f3 = ttk.Frame(f2)
f3.pack(fill="both",side='left',expand=True)

#lower-right middle frame
f4 = ttk.Frame(f2)
f4.pack(fill="both",side='right',expand=True)

#Format and Merge label frame
lf2 = ttk.Labelframe(f3, text = "Format & Merge", bootstyle="light")
lf2.pack(fill='both', expand=True, padx = x1, pady = y1)

#Import label frame
lf3 = ttk.Labelframe(f4, text = "Import", bootstyle="light")
lf3.pack(fill='both', expand=True, padx = x1, pady = y1)

#Info label frame
lf4 = ttk.Labelframe(f4, text = "Information", bootstyle="light")
lf4.pack(fill='both', expand=True, padx = x1, pady = y1)



ttk.Button(lf3, text="Form Import", command=open_import_form, bootstyle=INFO).pack(fill='x', padx = x1, pady = y1)
#.grid(row=1,column=0,padx=x1,pady=y1,sticky="nsew")
ttk.Button(lf3, text="CSV Import", command=open_import_csv, bootstyle=INFO).pack(fill='x', padx = x1, pady = y1)
#.grid(row=2,column=0,padx=x1,pady=y1,sticky="nsew")
ttk.Button(lf4, text="View ID-Name Key", command=open_GUI_idnametool, bootstyle=SECONDARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf4, text="View Database Schema", bootstyle=SECONDARY).pack(fill='x', padx = x1, pady = y1)
#.grid(row=1,column=2,padx=x1,pady=y1,sticky="nsew")


ttk.Button(lf2, text="Extract CSVs from Excel", command=open_format_exceltocsv, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)

ttk.Button(lf2, text="Format Kestral CSV", command=open_format_kestral, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)

ttk.Button(lf2, text="Format DataQ-MyChron CSV", command=open_format_dataqmychronformat, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Parse DataQ Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Parse MyChron Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
ttk.Button(lf2, text="Merge DataQ & MyChron Data", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)

#ttk.Button(lf2, text="Format Datalogger Files", command=open_format_dataqmychronmerge, bootstyle=PRIMARY).pack(fill='x', padx = x1, pady = y1)
######
mb1 = ttk.Menubutton(lf2, text="Format Datalogger File", bootstyle=PRIMARY)
mb1.pack(fill='x', padx = x1, pady = y1)

m1 = ttk.Menu(mb1, tearoff=0)
mb1["menu"] = m1

m1.add_command(label="Parse DataQ", command=open_format_dataqparse)
m1.add_command(label="Parse MyChron", command=open_format_mychronparse)
m1.add_command(label="Merge DataQ & MyChron Data", command=open_format_dataqmychronmerge)
m1.add_command(label="Format MaxxECU File", command=open_format_dataqmychronmerge) #CHANGE COMMAND


root.mainloop()


#.pack(pady=5)

