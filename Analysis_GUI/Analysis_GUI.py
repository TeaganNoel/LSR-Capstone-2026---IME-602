import tkinter as tk
from tkinter import ttk, messagebox

import ttkbootstrap as tb

from backend.database import (
    load_all_telemetry,
    list_tests,
    load_test_metadata,
    DatabaseConnectionError,
)
from backend.data_loader import (
    normalize_dataq_mychron,
    normalize_kestrel,
)
from backend.preprocessing import create_unified_context
from backend.metrics import (
    compute_all_metrics,
    compute_basic_run_metrics_for_many,
    compute_speed_series,
    estimate_drag_force_from_pitot,
)
from backend.plotting import (
    plot_single_run,
    plot_multi_run,
    metrics_to_table,
)
from backend.validation import (
    validate_all,
    ValidationError,
    ValidationWarning,
)

# Assumed reference area for drag coefficient (m²)
A_REF_M2 = 0.5


class App(tb.Window):
    def __init__(self):
        super().__init__(themename="cyborg")
        self.title("LSR Analysis GUI")
        self.geometry("1400x900")

        container = ttk.Frame(self)
        container.pack(fill="both", expand=True)
        container.rowconfigure(0, weight=1)
        container.columnconfigure(0, weight=1)

        self.frames = {}
        for F in (MainMenu, SingleRunViewer, MultiRunViewer, HistoricalExplorer, TestIDFinder):
            frame = F(parent=container, controller=self)
            self.frames[F] = frame
            frame.grid(row=0, column=0, sticky="nsew")

        self.show_frame(MainMenu)

    def show_frame(self, page):
        frame = self.frames[page]
        frame.tkraise()

    def set_single_run_test_id(self, test_id: str):
        frame: SingleRunViewer = self.frames[SingleRunViewer]
        frame.test_id_var.set(test_id)
        self.show_frame(SingleRunViewer)


class MainMenu(ttk.Frame):
    def __init__(self, parent, controller: App):
        super().__init__(parent)
        self.controller = controller

        ttk.Label(self, text="LSR Analysis Tool", font=("Helvetica", 28)).pack(pady=40)

        ttk.Button(self, text="Single Run Viewer",
                   command=lambda: controller.show_frame(SingleRunViewer),
                   width=30).pack(pady=10)

        ttk.Button(self, text="Multi‑Run Comparison",
                   command=lambda: controller.show_frame(MultiRunViewer),
                   width=30).pack(pady=10)

        ttk.Button(self, text="Historical Data Explorer",
                   command=lambda: controller.show_frame(HistoricalExplorer),
                   width=30).pack(pady=10)

        ttk.Button(self, text="Test ID Finder",
                   command=lambda: controller.show_frame(TestIDFinder),
                   width=30).pack(pady=10)


class SingleRunViewer(ttk.Frame):
    def __init__(self, parent, controller: App):
        super().__init__(parent)
        self.controller = controller

        ttk.Label(self, text="Single Run Viewer", font=("Helvetica", 20)).pack(pady=10)

        entry_frame = ttk.Frame(self)
        entry_frame.pack(pady=5)
        ttk.Label(entry_frame, text="Test ID:").pack(side="left", padx=5)

        self.test_id_var = tk.StringVar()
        ttk.Entry(entry_frame, textvariable=self.test_id_var, width=20).pack(side="left")

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=5)
        ttk.Button(btn_frame, text="Analyze Run", command=self.analyze_run).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Back", command=lambda: controller.show_frame(MainMenu)).pack(side="left", padx=5)

        ttk.Label(self, text="Plot will open in your browser", font=("Helvetica", 12)).pack(pady=10)

        # Bottom area: metrics + metadata
        tables_frame = ttk.Frame(self)
        tables_frame.pack(fill="both", expand=False, padx=10, pady=10)

        # Metrics table
        metrics_frame = ttk.Frame(tables_frame)
        metrics_frame.pack(side="left", fill="both", expand=True)

        ttk.Label(metrics_frame, text="Run Metrics", font=("Helvetica", 14)).pack(anchor="w")

        self.metrics_tree = ttk.Treeview(
            metrics_frame,
            columns=("Metric", "Value", "Units"),
            show="headings",
            height=14,
        )
        self.metrics_tree.heading("Metric", text="Metric")
        self.metrics_tree.heading("Value", text="Value")
        self.metrics_tree.heading("Units", text="Units")
        self.metrics_tree.column("Metric", width=220, anchor="w")
        self.metrics_tree.column("Value", width=150, anchor="center")
        self.metrics_tree.column("Units", width=80, anchor="center")
        self.metrics_tree.pack(fill="both", expand=True, pady=5)

        # Metadata table
        meta_frame = ttk.Frame(tables_frame)
        meta_frame.pack(side="left", fill="both", expand=True, padx=(10, 0))

        ttk.Label(meta_frame, text="Run Metadata", font=("Helvetica", 14)).pack(anchor="w")

        self.meta_tree = ttk.Treeview(
            meta_frame,
            columns=("Field", "Value"),
            show="headings",
            height=10,
        )
        self.meta_tree.heading("Field", text="Field")
        self.meta_tree.heading("Value", text="Value")
        self.meta_tree.column("Field", width=150, anchor="w")
        self.meta_tree.column("Value", width=250, anchor="w")
        self.meta_tree.pack(fill="both", expand=False, pady=5)

        # Multi-line Notes box
        ttk.Label(meta_frame, text="Notes:", font=("Helvetica", 12)).pack(anchor="w")
        self.notes_box = tk.Text(meta_frame, height=4, wrap="word")
        self.notes_box.pack(fill="both", expand=True, pady=5)

    # ------------------------------------------------------------
    # SINGLE RUN ANALYSIS
    # ------------------------------------------------------------
    def analyze_run(self):
        test_id = self.test_id_var.get().strip()
        if not test_id:
            messagebox.showerror("Error", "Please enter a Test ID.")
            return

        # Load telemetry
        try:
            raw = load_all_telemetry(test_id)
        except DatabaseConnectionError as e:
            messagebox.showerror("Database Error", str(e))
            return

        # Normalize
        dfs = {
            "dataq_mychron": normalize_dataq_mychron(raw["dataq_mychron"]),
            "kestrel": normalize_kestrel(raw["kestrel"]),
        }

        # Strict-mode validation
        try:
            validate_all(dfs)
        except ValidationWarning as w:
            messagebox.showwarning("Warning", str(w))
        except ValidationError as e:
            messagebox.showerror("Data Error", str(e))
            return

        # Compute metrics
        ctx = create_unified_context(dfs)
        metrics = compute_all_metrics(ctx)

        # Convert distance to top speed from meters → miles (in-place)
        if "distance_to_vmax_m" in metrics and metrics["distance_to_vmax_m"] is not None:
            try:
                metrics["distance_to_vmax_m"] = float(metrics["distance_to_vmax_m"]) / 1609.34
            except Exception:
                pass

        # Compute peak drag coefficient (Cd) if possible
        try:
            drag_force = estimate_drag_force_from_pitot(dfs["dataq_mychron"], dfs.get("kestrel"))
            rho = metrics.get("air_density_kg_m3", None)
            if drag_force is not None and not drag_force.empty and rho and rho > 0 and A_REF_M2 > 0:
                speed_mps = compute_speed_series(dfs["dataq_mychron"])
                # Align speed with drag index
                speed_on_drag = speed_mps.reindex(drag_force.index, method="nearest")
                valid = (speed_on_drag > 0)
                cd_series = 2.0 * drag_force[valid] / (rho * speed_on_drag[valid] ** 2 * A_REF_M2)
                cd_series = cd_series.replace([float("inf"), float("-inf")], float("nan")).dropna()
                if not cd_series.empty:
                    metrics["peak_drag_coefficient"] = float(cd_series.max())
        except Exception:
            # If anything goes wrong, just skip Cd
            pass

        # Plot in browser
        fig = plot_single_run(dfs)
        fig.show()

        # Populate metrics table
        for row in self.metrics_tree.get_children():
            self.metrics_tree.delete(row)

        df_metrics = metrics_to_table(metrics)
        for _, row in df_metrics.iterrows():
            val = row["Value"]
            if isinstance(val, float):
                val = f"{val:.3f}"
            self.metrics_tree.insert(
                "",
                "end",
                values=(row["Metric"], val, row["Units"]),
            )

        # Populate metadata table
        for row in self.meta_tree.get_children():
            self.meta_tree.delete(row)

        self.notes_box.delete("1.0", tk.END)

        try:
            meta_df = load_test_metadata(test_id)
        except DatabaseConnectionError as e:
            messagebox.showerror("DB Error", str(e))
            return

        if not meta_df.empty:
            r = meta_df.iloc[0]
            meta_rows = [
                ("TestID", r.get("testID")),
                ("Date", r.get("date")),
                ("Start Time", r.get("datetime_attr")),
                ("Duration", r.get("duration")),
                ("Rider", r.get("riderID")),
                ("Vehicle", r.get("vehicleID")),
            ]
            for field, value in meta_rows:
                self.meta_tree.insert("", "end", values=(field, value))

            # Notes in multi-line box
            self.notes_box.insert("1.0", r.get("notes", ""))


class MultiRunViewer(ttk.Frame):
    def __init__(self, parent, controller: App):
        super().__init__(parent)
        self.controller = controller

        ttk.Label(self, text="Multi‑Run Comparison", font=("Helvetica", 20)).pack(pady=20)

        entry_frame = ttk.Frame(self)
        entry_frame.pack(pady=10)
        ttk.Label(entry_frame, text="Test IDs (comma‑separated):").pack(side="left", padx=5)

        self.ids_var = tk.StringVar()
        ttk.Entry(entry_frame, textvariable=self.ids_var, width=40).pack(side="left")

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Compare Runs", command=self.compare_runs).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Back", command=lambda: controller.show_frame(MainMenu)).pack(side="left", padx=5)

        self.tree = ttk.Treeview(self, columns=("TestID", "Metric", "Value"), show="headings", height=14)
        self.tree.heading("TestID", text="Test ID")
        self.tree.heading("Metric", text="Metric")
        self.tree.heading("Value", text="Value")
        self.tree.pack(fill="both", expand=True, padx=20, pady=10)

    def compare_runs(self):
        raw_ids = self.ids_var.get().strip()
        if not raw_ids:
            messagebox.showerror("Error", "Enter at least one Test ID.")
            return

        ids = [part.strip() for part in raw_ids.split(",") if part.strip()]
        if not ids:
            messagebox.showerror("Error", "No valid Test IDs found.")
            return

        dfs_by_id = {}
        for tid in ids:
            try:
                raw = load_all_telemetry(tid)
            except DatabaseConnectionError as e:
                messagebox.showerror("DB Error", f"Error loading test {tid}: {e}")
                return

            dfs_by_id[tid] = {
                "dataq_mychron": normalize_dataq_mychron(raw["dataq_mychron"]),
                "kestrel": normalize_kestrel(raw["kestrel"]),
            }

        fig = plot_multi_run(dfs_by_id)
        fig.show()

        metrics_by_id = compute_basic_run_metrics_for_many(dfs_by_id)

        for row in self.tree.get_children():
            self.tree.delete(row)

        for tid, m in metrics_by_id.items():
            for k, v in m.items():
                if isinstance(v, float):
                    v = f"{v:.3f}"
                self.tree.insert("", "end", values=(tid, k, v))


class HistoricalExplorer(ttk.Frame):
    def __init__(self, parent, controller: App):
        super().__init__(parent)
        self.controller = controller

        ttk.Label(self, text="Historical Data Explorer", font=("Helvetica", 20)).pack(pady=20)

        entry_frame = ttk.Frame(self)
        entry_frame.pack(pady=10)
        ttk.Label(entry_frame, text="Test ID:").pack(side="left", padx=5)

        self.test_id_var = tk.StringVar()
        ttk.Entry(entry_frame, textvariable=self.test_id_var, width=20).pack(side="left")

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Plot Speed vs Time", command=self.plot_speed_time).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Back", command=lambda: controller.show_frame(MainMenu)).pack(side="left", padx=5)

    def plot_speed_time(self):
        test_id = self.test_id_var.get().strip()
        if not test_id:
            messagebox.showerror("Error", "Please enter a Test ID.")
            return

        try:
            raw = load_all_telemetry(test_id)
        except DatabaseConnectionError as e:
            messagebox.showerror("DB Error", str(e))
            return

        dfs = {
            "dataq_mychron": normalize_dataq_mychron(raw["dataq_mychron"]),
            "kestrel": normalize_kestrel(raw["kestrel"]),
        }

        fig = plot_single_run(dfs)
        fig.update_layout(title=f"Historical View – Test {test_id}")
        fig.show()


class TestIDFinder(ttk.Frame):
    def __init__(self, parent, controller: App):
        super().__init__(parent)
        self.controller = controller

        ttk.Label(self, text="Test ID Finder", font=("Helvetica", 20)).pack(pady=20)

        self.tree = ttk.Treeview(
            self,
            columns=("testID", "date", "datetime", "riderID", "vehicleID"),
            show="headings",
            height=18,
        )
        for col, label in [
            ("testID", "Test ID"),
            ("date", "Date"),
            ("datetime", "Start Time"),
            ("riderID", "Rider"),
            ("vehicleID", "Vehicle"),
        ]:
            self.tree.heading(col, text=label)
        self.tree.pack(fill="both", expand=True, padx=20, pady=10)

        btn_frame = ttk.Frame(self)
        btn_frame.pack(pady=10)
        ttk.Button(btn_frame, text="Load Recent Tests", command=self.load_tests).pack(side="left", padx=5)
        ttk.Button(btn_frame, text="Back", command=lambda: controller.show_frame(MainMenu)).pack(side="left", padx=5)

        self.tree.bind("<Double-1>", self.on_double_click)

    def load_tests(self):
        try:
            df = list_tests(limit=200)
        except DatabaseConnectionError as e:
            messagebox.showerror("DB Error", str(e))
            return

        for row in self.tree.get_children():
            self.tree.delete(row)

        for _, r in df.iterrows():
            self.tree.insert(
                "",
                "end",
                values=(
                    r["testID"],
                    r["date"],
                    r["datetime_attr"],
                    r["riderID"],
                    r["vehicleID"],
                ),
            )

    def on_double_click(self, event):
        item = self.tree.selection()
        if not item:
            return
        vals = self.tree.item(item[0], "values")
        test_id = vals[0]
        self.controller.set_single_run_test_id(test_id)


if __name__ == "__main__":
    app = App()
    app.mainloop()
