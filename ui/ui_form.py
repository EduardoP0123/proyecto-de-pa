import sys
import os
from pathlib import Path
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
from datetime import datetime
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

        date_frame = ttk.LabelFrame(main_frame, text="Mes y ventana horaria", padding="10")
        date_frame.grid(row=2, column=0, columnspan=4, sticky="ew", pady=15)
        date_frame.columnconfigure(1, weight=1)

        ttk.Label(date_frame, text="Selecciona una fecha del mes:").grid(row=0, column=0, sticky="w")
        self.date_picker = DateEntry(date_frame, date_pattern="dd/mm/yyyy")
        self.date_picker.grid(row=0, column=1, sticky="w", padx=(8, 18))

        # Hora de inicio
        ttk.Label(date_frame, text="Hora inicio (HH:MM):").grid(row=0, column=2, sticky="e")
        self.start_hour = ttk.Combobox(date_frame, values=[f"{h:02d}" for h in range(24)], width=3, state="readonly")
        self.start_min = ttk.Combobox(date_frame, values=["00", "15", "30", "45"], width=3, state="readonly")
        self.start_hour.set("00")
        self.start_min.set("00")
        self.start_hour.grid(row=0, column=3, sticky="w")
        ttk.Label(date_frame, text=":").grid(row=0, column=4, sticky="w")
        self.start_min.grid(row=0, column=5, sticky="w", padx=(0, 18))

        # Hora de fin
        ttk.Label(date_frame, text="Hora fin (HH:MM):").grid(row=0, column=6, sticky="e")
        self.end_hour = ttk.Combobox(date_frame, values=[f"{h:02d}" for h in range(24)], width=3, state="readonly")
        self.end_min = ttk.Combobox(date_frame, values=["00", "15", "30", "45"], width=3, state="readonly")
        self.end_hour.set("00")
        self.end_min.set("15")
        self.end_hour.grid(row=0, column=7, sticky="w")
        ttk.Label(date_frame, text=":").grid(row=0, column=8, sticky="w")
        self.end_min.grid(row=0, column=9, sticky="w")

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

        fecha = self.date_picker.get_date()
        mes_usuario = fecha.month
        año_usuario = fecha.year
        start_time = f"{self.start_hour.get()}:{self.start_min.get()}"
        end_time = f"{self.end_hour.get()}:{self.end_min.get()}"

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
                ok, msg, results = self.csv_processor.analyze_folder(
                    Path(folder_path),
                    mes_usuario=mes_usuario,
                    año_usuario=año_usuario,
                    start_time=start_time,
                    end_time=end_time,
                    progress_cb=progress_cb
                )
                self.root.after(0, lambda: self.on_analysis_done(ok, msg, results))
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