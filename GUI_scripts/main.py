import sys
import os

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)

import ttkbootstrap as tb
from ttkbootstrap.constants import *
import tkinter as tk
from tkinter import ttk, messagebox

import plotly.io as pio
from PIL import Image, ImageTk
import io

# Backend imports
from backend.database import load_all_telemetry, load_test_metadata
from backend.data_loader import (
    normalize_mychron,
    normalize_dataq_mychron,
    normalize_dataq,
    normalize_kestrel,
)
from backend.preprocessing import create_unified_context
from backend.metrics import compute_all_metrics
from backend.plotting import plot_single_run, metrics_to_table


# -------------------------------------------------------------------
# Utility: Convert Plotly figure → Tkinter PhotoImage
# -------------------------------------------------------------------
def plotly_to_tk(fig, width=900, height=400):
    """
    Convert a Plotly figure to a Tkinter-compatible PhotoImage.
    """
    img_bytes = pio.to_image(fig, format="png", width=width, height=height)
    img = Image.open(io.BytesIO(img_bytes))
    return ImageTk.PhotoImage(img)


# -------------------------------------------------------------------
# GUI Application
# -------------------------------------------------------------------
class RunViewerApp(tb.Window):
    def __init__(self):
        super().__init__(title="LSR Unified Run Viewer", themename="darkly")
        self.geometry("1200x800")

        # Top frame: Test ID input
        top_frame = ttk.Frame(self)
        top_frame.pack(fill=X, pady=10)

        ttk.Label(top_frame, text="Enter Test ID:", font=("Segoe UI", 12)).pack(side=LEFT, padx=5)

        self.testid_var = tk.StringVar()
        ttk.Entry(top_frame, textvariable=self.testid_var, width=15).pack(side=LEFT, padx=5)

        ttk.Button(top_frame, text="Load Run", bootstyle=PRIMARY, command=self.load_run).pack(side=LEFT, padx=10)

        # Main content frames
        self.plot_frame = ttk.Frame(self)
        self.plot_frame.pack(fill=BOTH, expand=True, padx=10, pady=10)

        self.metrics_frame = ttk.Frame(self)
        self.metrics_frame.pack(fill=X, padx=10, pady=10)

        # Placeholder for plot image
        self.plot_label = ttk.Label(self.plot_frame)
        self.plot_label.pack()

        # Metrics table
        self.metrics_table = ttk.Treeview(self.metrics_frame, columns=("Metric", "Value"), show="headings", height=8)
        self.metrics_table.heading("Metric", text="Metric")
        self.metrics_table.heading("Value", text="Value")
        self.metrics_table.pack(fill=X)

        # Warnings
        self.warning_label = ttk.Label(self.metrics_frame, text="", foreground="orange")
        self.warning_label.pack(pady=5)

    # -------------------------------------------------------------------
    # Load Run Handler
    # -------------------------------------------------------------------
    def load_run(self):
        test_id_str = self.testid_var.get().strip()

        if not test_id_str.isdigit():
            messagebox.showerror("Invalid Input", "Test ID must be a number.")
            return

        test_id = int(test_id_str)

        try:
            # Load telemetry
            dfs = load_all_telemetry(test_id)

            # Normalize
            dfs["mychron"] = normalize_mychron(dfs["mychron"])
            dfs["dataq_mychron"] = normalize_dataq_mychron(dfs["dataq_mychron"])
            dfs["dataq"] = normalize_dataq(dfs["dataq"])
            dfs["kestrel"] = normalize_kestrel(dfs["kestrel"])

            # Preprocess
            context = create_unified_context(dfs)

            # Metrics
            metrics = compute_all_metrics(context)

            # Plot
            fig = plot_single_run(dfs)
            tk_img = plotly_to_tk(fig)
            self.plot_label.configure(image=tk_img)
            self.plot_label.image = tk_img  # prevent garbage collection

            # Metrics table
            self.update_metrics_table(metrics)

            # Warnings
            warnings = metrics.get("sanity_warnings", [])
            if warnings:
                self.warning_label.configure(text="Warnings: " + "; ".join(warnings))
            else:
                self.warning_label.configure(text="")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to load run:\n{e}")

    # -------------------------------------------------------------------
    # Update Metrics Table
    # -------------------------------------------------------------------
    def update_metrics_table(self, metrics: dict):
        for row in self.metrics_table.get_children():
            self.metrics_table.delete(row)

        df = metrics_to_table(metrics)
        for _, row in df.iterrows():
            self.metrics_table.insert("", END, values=(row["Metric"], row["Value"]))


# -------------------------------------------------------------------
# Run App
# -------------------------------------------------------------------
if __name__ == "__main__":
    app = RunViewerApp()
    app.mainloop()
