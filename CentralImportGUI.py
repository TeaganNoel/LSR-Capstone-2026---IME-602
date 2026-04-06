#Author: Mason Allen
#Description: Creates a GUI for operating all formatting and import scripts
#Created Date: 4/4/2026
#Last Updated Date: 4/4/2026

import tkinter as tk
import ttkbootstrap as ttk
from ttkbootstrap.constants import *

#import external python scripts
from import_scripts import ImportForm2
from import_scripts import ImportCSV2

from formatting_scripts import Dataq_MycronFormatting1
from formatting_scripts import ExcelToCSV2
from formatting_scripts import KestralFormatting1

from GUI_scripts import IDNameKeyGenerator


#define padding vars
x1 = 5
x2 = 8
y1 = 5
y2 = 8


def ImportForm():
    ImportForm2

def ImportCSV():
    ImportCSV2
    



root = ttk.Window(themename="darkly")

b1 = ttk.Button(root, text="DB Form Import", command=ImportForm, bootstyle=SUCCESS)
b1.pack(side=LEFT, padx=x1, pady=y1)
