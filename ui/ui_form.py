import tkinter as tk
import os
from pathlib import Path
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
from datetime import datetime
import sys
import pandas as pd
from collections import defaultdict
import threading

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from src.ui_components import run_ui

class CSVUploaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Procesador de Carpeta CSV - BILLREAD")
        self.root.geometry("820x560")
        self.root.resizable(True, True)

        workspace_path = Path.home() / "Downloads" / f"BILLREAD_WORKSPACE_{Path(__file__).parent.name}"
        workspace_path.mkdir(parents=True, exist_ok=True)
        self.csv_processor = run_ui(workspace_path)

        self.create_widgets()

    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.N, tk.S, tk.E, tk.W))
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)

        title_label = ttk.Label(main_frame, text="Procesador de Carpeta CSV - BILLREAD", font=("Arial", 16, "bold"))
        title_label.grid(row=0, column=0, columnspan=4, pady=(0, 20))

        ttk.Label(main_frame, text="Carpeta:").grid(row=1, column=0, sticky="w", pady=5)
        self.folder_path = ttk.Entry(main_frame)
        self.folder_path.grid(row=1, column=1, columnspan=2, sticky="ew", padx=(10, 10))
        ttk.Button(main_frame, text="Seleccionar Carpeta", command=self.browse_folder).grid(row=1, column=3, sticky="e")

        date_frame = ttk.LabelFrame(main_frame, text="Rango de análisis", padding="10")
        date_frame.grid(row=2, column=0, columnspan=4, sticky="ew", pady=15)
        for c in range(10):
            date_frame.columnconfigure(c, weight=0)
        date_frame.columnconfigure(1, weight=1)
        date_frame.columnconfigure(6, weight=1)

        # ---------------- Inicio (FECHA + HORA) ----------------
        ttk.Label(date_frame, text="Fecha de inicio:").grid(row=0, column=0, sticky="w")
        now = datetime.now()
        self.start_date = DateEntry(date_frame, date_pattern="dd/mm/yyyy")
        self.start_date.set_date(now)
        self.start_date.grid(row=0, column=1, sticky="w", padx=(0, 18))

        ttk.Label(date_frame, text="Hora inicio (HH:MM):").grid(row=0, column=3, sticky="e")
        self.start_hour = ttk.Combobox(date_frame, values=[f"{h:02d}" for h in range(24)], width=3, state="readonly")
        self.start_min = ttk.Combobox(date_frame, values=["00", "15", "30", "45"], width=3, state="readonly")
        self.start_hour.set("00")
        self.start_min.set("00")
        self.start_hour.grid(row=0, column=4, sticky="w")
        ttk.Label(date_frame, text=":").grid(row=0, column=5, sticky="w")
        self.start_min.grid(row=0, column=6, sticky="w", padx=(0, 18))

        # ---------------- Fin (FECHA + HORA) ----------------
        ttk.Label(date_frame, text="Fecha de fin:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.end_date = DateEntry(date_frame, date_pattern="dd/mm/yyyy")
        self.end_date.set_date(now)
        self.end_date.grid(row=1, column=1, sticky="w", padx=(0, 0), pady=(8, 0))

        ttk.Label(date_frame, text="Hora fin (HH:MM):").grid(row=1, column=3, sticky="e", pady=(8, 0))
        self.end_hour = ttk.Combobox(date_frame, values=[f"{h:02d}" for h in range(24)], width=3, state="readonly")
        self.end_min = ttk.Combobox(date_frame, values=["00", "15", "30", "45"], width=3, state="readonly")
        self.end_hour.set("00")
        self.end_min.set("15")
        self.end_hour.grid(row=1, column=4, sticky="w", pady=(8, 0))
        ttk.Label(date_frame, text=":").grid(row=1, column=5, sticky="w", pady=(8, 0))
        self.end_min.grid(row=1, column=6, sticky="w", pady=(8, 0))

        ttk.Button(main_frame, text="Analizar Carpeta", command=self.analyze_folder).grid(row=3, column=0, columnspan=4, pady=15)

        info_frame = ttk.LabelFrame(main_frame, text="Información del Procesamiento", padding="10")
        info_frame.grid(row=4, column=0, columnspan=4, sticky="nsew")
        main_frame.rowconfigure(4, weight=1)
        info_frame.columnconfigure(0, weight=1)
        info_frame.rowconfigure(0, weight=1)

        self.info_text = tk.Text(info_frame, height=12, wrap=tk.WORD, state="disabled", bg="#f7f7f7")
        self.info_text.grid(row=0, column=0, sticky="nsew")
        sb = ttk.Scrollbar(info_frame, orient=tk.VERTICAL, command=self.info_text.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.info_text.configure(yscrollcommand=sb.set)

        actions = ttk.Frame(main_frame)
        actions.grid(row=5, column=0, columnspan=4, pady=10)
        self.export_excel_btn = ttk.Button(actions, text="Exportar Excel Multi-Hoja", command=self.export_excel, state="disabled")
        self.export_csv_btn = ttk.Button(actions, text="Exportar CSV Combinado", command=self.export_csv, state="disabled")
        clear_btn = ttk.Button(actions, text="Limpiar", command=self.clear_data)
        self.export_excel_btn.grid(row=0, column=0, padx=6)
        self.export_csv_btn.grid(row=0, column=1, padx=6)
        clear_btn.grid(row=0, column=2, padx=6)

    def browse_folder(self):
        folder_path = filedialog.askdirectory(title="Seleccionar carpeta con archivos CSV")
        if folder_path:
            self.folder_path.delete(0, 'end')
            self.folder_path.insert(0, folder_path)

    def analyze_folder(self):
        folder_path = self.folder_path.get()
        if not folder_path or not os.path.exists(folder_path):
            messagebox.showerror("Error", "Selecciona una carpeta válida")
            return

        # Tomar fecha de inicio (se usa solo su mes y año para el backend actual)
        try:
            start_date = self.start_date.get_date()
            mes_usuario = int(start_date.month)
            año_usuario = int(start_date.year)
        except Exception:
            messagebox.showerror("Error", "Selecciona una fecha de inicio válida")
            return

        # Fecha fin (solo UI, servirá para recortar y para decidir meses a procesar)
        end_date_ui = self.end_date.get_date()

        start_time = f"{self.start_hour.get()}:{self.start_min.get()}"
        end_time = f"{self.end_hour.get()}:{self.end_min.get()}"

        # Construir datetimes exactos de usuario para recortar al final
        try:
            sh, sm = map(int, start_time.split(":"))
            eh, em = map(int, end_time.split(":"))
            user_start_dt = datetime(start_date.year, start_date.month, start_date.day, sh, sm, 0)
            user_end_dt = datetime(end_date_ui.year, end_date_ui.month, end_date_ui.day, eh, em, 0)
        except Exception:
            messagebox.showerror("Error", "Horas inválidas")
            return
        if user_end_dt < user_start_dt:
            messagebox.showerror("Error", "La fecha/hora de fin debe ser posterior al inicio")
            return

        # Limpia y muestra mensaje inicial
        self.info_text.configure(state="normal")
        self.info_text.delete(1.0, tk.END)
        self.info_text.insert(tk.END, "Procesando, por favor espera...\n")
        self.info_text.configure(state="disabled")
        # Deshabilita botones mientras procesa
        self.export_excel_btn.configure(state="disabled")
        self.export_csv_btn.configure(state="disabled")

        def progress_cb(msg: str):
            # Actualiza la UI desde el hilo principal
            self.root.after(0, lambda: self.append_info(msg))

        def worker():
            try:
                # Si el rango cruza meses, ejecuta por mes y combina (sin tocar el backend)
                def month_span(y1, m1, y2, m2):
                    y, m = y1, m1
                    while (y < y2) or (y == y2 and m <= m2):
                        yield y, m
                        if m == 12:
                            y, m = y + 1, 1
                        else:
                            m += 1

                months = list(month_span(start_date.year, start_date.month, end_date_ui.year, end_date_ui.month))
                monthly_dfs = []
                all_details = []
                total_files = total_processed = total_errors = 0
                last_folder = str(Path(folder_path))
                last_msgs = []

                for (yy, mm) in months:
                    # Tiempos por mes para no recortar días completos indebidamente
                    if (yy, mm) == (start_date.year, start_date.month) and (yy, mm) == (end_date_ui.year, end_date_ui.month):
                        s_t, e_t = start_time, end_time
                    elif (yy, mm) == (start_date.year, start_date.month):
                        s_t, e_t = start_time, "23:59"
                    elif (yy, mm) == (end_date_ui.year, end_date_ui.month):
                        s_t, e_t = "00:00", end_time
                    else:
                        s_t, e_t = "00:00", "23:59"

                    ok, msg, results = self.csv_processor.analyze_folder(
                         Path(folder_path),
                         mes_usuario=mm,
                         año_usuario=yy,
                         start_time=s_t,
                         end_time=e_t,
                         progress_cb=progress_cb
                     )
                    last_msgs.append(msg)
                    if ok and hasattr(self.csv_processor, "combined_df") and self.csv_processor.combined_df is not None:
                        monthly_dfs.append(self.csv_processor.combined_df.copy())
                        all_details.extend(results.get("file_details", []))
                        total_files += results.get("total_files", 0)
                        total_processed += results.get("processed_files", 0)
                        total_errors += results.get("error_files", 0)
                        last_folder = results.get("folder", last_folder)
                    else:
                        total_files += results.get("total_files", 0) if results else 0
                        total_errors += 1

                if not monthly_dfs:
                    self.root.after(0, lambda: self.show_error("No se generaron datos"))
                    return

                combined = pd.concat(monthly_dfs, ignore_index=True)
                combined = combined[(combined["timestamp"] >= user_start_dt) & (combined["timestamp"] <= user_end_dt)]
                combined = combined.sort_values(["company", "timestamp"]).reset_index(drop=True)
                self.csv_processor.combined_df = combined

                # ---- Deduplicar detalles por archivo (evita listar dos veces por mes) ----
                fmt = "%d/%m/%Y %H:%M"
                agg = defaultdict(lambda: {
                    "filename": None, "rows": 0, "success": False,
                    "kwh_values": 0, "kvar_values": 0,
                    "start_date": None, "end_date": None, "error": None
                })
                for d in all_details:
                    name = d.get("filename")
                    if not name:
                        continue
                    a = agg[name]
                    a["filename"] = name
                    a["rows"] += int(d.get("rows", 0))
                    a["success"] = a["success"] or bool(d.get("success", False))
                    a["kwh_values"] += int(d.get("kwh_values", 0))
                    a["kvar_values"] += int(d.get("kvar_values", 0))
                    # tomar mínimo inicio y máximo fin disponibles
                    try:
                        sd = datetime.strptime(str(d.get("start_date")), fmt) if d.get("start_date") else None
                        ed = datetime.strptime(str(d.get("end_date")), fmt) if d.get("end_date") else None
                    except Exception:
                        sd = ed = None
                    if sd:
                        if a["start_date"] is None or sd < datetime.strptime(a["start_date"], fmt):
                            a["start_date"] = sd.strftime(fmt)
                    if ed:
                        if a["end_date"] is None or ed > datetime.strptime(a["end_date"], fmt):
                            a["end_date"] = ed.strftime(fmt)
                    if d.get("error"):
                        a["error"] = (a["error"] + " | " if a["error"] else "") + str(d["error"])
                dedup_details = list(agg.values())
                # Ajustar rango mostrado por archivo al rango exacto del usuario si aplica
                for a in dedup_details:
                    a["start_date"] = user_start_dt.strftime(fmt) if a["start_date"] else user_start_dt.strftime(fmt)
                    a["end_date"] = user_end_dt.strftime(fmt) if a["end_date"] else user_end_dt.strftime(fmt)

                # Totales únicos por archivo
                total_files_u = len(dedup_details)
                processed_u = sum(1 for x in dedup_details if x.get("success"))
                error_u = total_files_u - processed_u

                results_agg = {
                    "folder": last_folder,
                    "total_files": total_files_u,
                    "processed_files": processed_u,
                    "error_files": error_u,
                    "date_range": {
                        "start": user_start_dt.strftime("%d/%m/%Y %H:%M"),
                        "end": user_end_dt.strftime("%d/%m/%Y %H:%M"),
                    },
                    "combined_stats": {
                        "total_rows": int(combined.shape[0]),
                        "total_columns": int(combined.shape[1]),
                        "total_kwh_values": int(pd.notna(combined["kwh"]).sum()),
                        "total_kvar_values": int(pd.notna(combined["kvarh"]).sum()),
                    },
                    "file_details": dedup_details,
                    "errors": []  # ya contabilizados
                }
                self.root.after(0, lambda: self.on_analysis_done(True, "Procesamiento completado", results_agg))
            except Exception as e:
                self.root.after(0, lambda: self.show_error(str(e)))

        threading.Thread(target=worker, daemon=True).start()

    def append_info(self, text: str):
        self.info_text.configure(state="normal")
        self.info_text.insert(tk.END, text.rstrip() + "\n")
        self.info_text.see(tk.END)
        self.info_text.configure(state="disabled")

    def show_error(self, error_msg):
        self.info_text.configure(state="normal")
        self.info_text.insert(tk.END, f"Error: {error_msg}\n")
        self.info_text.configure(state="disabled")
        messagebox.showerror("Error", error_msg)

    def on_analysis_done(self, ok, msg, results):
        if ok and results and results.get("combined_stats", {}).get("total_rows", 0) > 0:
            self.show_processing_info(results)
            self.export_excel_btn.configure(state="normal")
            self.export_csv_btn.configure(state="normal")
            messagebox.showinfo("Éxito", msg)
        else:
            self.show_error(msg)

    def show_processing_info(self, results):
        self.info_text.configure(state="normal")
        self.info_text.delete(1.0, tk.END)
        info = []
        info.append(f"Carpeta: {results['folder']}")
        info.append(f"Archivos encontrados: {results['total_files']}")
        info.append(f"Procesados: {results['processed_files']} | Con error: {results['error_files']}")
        info.append(f"Rango de fechas: {results['date_range']['start']} → {results['date_range']['end']}")
        info.append(f"Filas combinadas: {results['combined_stats']['total_rows']}")
        info.append("")
        info.append("Archivos:")
        for f in results['file_details']:
            if f.get('success'):
                info.append(f"  ✓ {f['filename']} - {f['rows']} filas ({f['start_date']} → {f['end_date']})")
            else:
                info.append(f"  ✗ {f['filename']} - {f.get('error','Error')}")
        self.info_text.insert(1.0, "\n".join(info))
        self.info_text.configure(state="disabled")

    def export_excel(self):
        # Corrección de verificación
        if not hasattr(self.csv_processor, 'combined_df') or self.csv_processor.combined_df is None:
            messagebox.showwarning("Advertencia", "Primero analiza una carpeta")
            return
        filename = filedialog.asksaveasfilename(defaultextension=".xlsx", filetypes=[("Excel", "*.xlsx")])
        if filename:
            ok, msg = self.csv_processor.export_excel_multi_sheet(filename)
            if ok:
                messagebox.showinfo("Éxito", msg)
            else:
                messagebox.showerror("Error", msg)

    def export_csv(self):
        # Corrección de verificación
        if not hasattr(self.csv_processor, 'combined_df') or self.csv_processor.combined_df is None:
            messagebox.showwarning("Advertencia", "Primero analiza una carpeta")
            return
        filename = filedialog.asksaveasfilename(defaultextension=".csv", filetypes=[("CSV", "*.csv")])
        if filename:
            ok, msg = self.csv_processor.export_combined_csv(filename)
            if ok:
                messagebox.showinfo("Éxito", msg)
            else:
                messagebox.showerror("Error", msg)

    def clear_data(self):
        self.folder_path.delete(0, 'end')
        if hasattr(self.csv_processor, 'clear_data'):
            self.csv_processor.clear_data()
        self.info_text.configure(state="normal")
        self.info_text.delete(1.0, tk.END)
        self.info_text.configure(state="disabled")
        self.export_excel_btn.configure(state="disabled")
        self.export_csv_btn.configure(state="disabled")

def main():
    root = tk.Tk()
    app = CSVUploaderApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()