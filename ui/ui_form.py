import os
import sys
import threading
import tkinter as tk
from pathlib import Path
import math
from tkinter import ttk, filedialog, messagebox
from tkcalendar import DateEntry
from datetime import datetime
from collections import defaultdict
import pandas as pd
import re

# Imagen (opcional, fallback si no hay Pillow)
try:
    from PIL import Image, ImageTk  # type: ignore[import]
except Exception:
    Image = ImageTk = None

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))
try:
    from src.ui_components import run_ui
except Exception:
    run_ui = None
from src.csv_processor import CSVProcessor


class CSVUploaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Lecturas KV2C - v2.0")
        # Escalado para pantallas FHD/4K
        try:
            self.root.tk.call("tk", "scaling", 1.25)
        except Exception:
            pass

        self._init_style()

        self.root.geometry("980x680")
        self.root.minsize(900, 600)
        self.root.rowconfigure(0, weight=1)
        self.root.columnconfigure(0, weight=1)

        # referencia del logo para evitar GC
        self.seg_logo_img = None

        # Procesador
        workspace_path = Path.home() / "Downloads" / "BILLREAD_WORKSPACE"
        workspace_path.mkdir(parents=True, exist_ok=True)
        self.csv_processor = run_ui(workspace_path) if run_ui else CSVProcessor(workspace_path)

        # UI
        self.create_widgets()
        self._build_statusbar()

    # ---------- Estilo ----------
    def _init_style(self):
        style = ttk.Style()
        # Tema estable y moderno
        try:
            style.theme_use("clam")
        except Exception:
            pass
        # Colores base
        self.COLOR_BG = "#0D1B2A"   # azul oscuro header
        self.COLOR_ACCENT = "#1B6F9B"
        self.COLOR_BTN = "#1F7A8C"

        style.configure("Header.TFrame", background=self.COLOR_BG)
        style.configure("Header.TLabel", background=self.COLOR_BG, foreground="white", font=("Segoe UI", 16, "bold"))
        style.configure("TLabel", font=("Segoe UI", 10))
        style.configure("TButton", font=("Segoe UI", 10))
        style.configure("Accent.TButton", font=("Segoe UI", 10, "bold"), foreground="white", background=self.COLOR_BTN)
        style.map("Accent.TButton", background=[("active", "#2390A6"), ("disabled", "#9fb6bf")])
        style.configure("Card.TFrame", padding=12)
        style.configure("Section.TLabelframe", padding=12)
        style.configure("Section.TLabelframe.Label", font=("Segoe UI", 11, "bold"))

    # ---------- Logo ----------
    def _load_seg_logo(self, max_h=56, max_w=220):
        """Carga y escala el logo buscando en ui/images y assets."""
        try:
            here = Path(__file__).resolve().parent
            roots = [
                here / "images",                    
                here.parent / "assets",             
                Path.cwd() / "ui" / "images",       
                Path.cwd() / "assets",
            ]
            candidates = [
                "seg.png", "SEG.png", "seg_logo.png",
                "seg.jpg", "SEG.jpg", "seg_logo.jpg",
                "seg.gif", "SEG.gif"
            ]
            for root in roots:
                for name in candidates:
                    p = root / name
                    if not p.exists():
                        continue
                    # Con Pillow (admite JPG/PNG/GIF y mejor escalado)
                    if Image and ImageTk:
                        img = Image.open(p).convert("RGBA")
                        r = min(max_h / img.height, max_w / img.width, 1.0)
                        new_size = (max(1, int(img.width * r)), max(1, int(img.height * r)))
                        img = img.resize(new_size, Image.LANCZOS)
                        return ImageTk.PhotoImage(img)
                    # Fallback sin Pillow: PhotoImage (PNG/GIF)
                    pic = tk.PhotoImage(file=str(p))
                    h, w = pic.height(), pic.width()
                    factor = max(1, math.ceil(max(h / max_h, w / max_w)))
                    if factor > 1:
                        pic = pic.subsample(factor, factor)
                    return pic
        except Exception as e:
            try:
                self.append_info(f"[LOGO] No se pudo cargar: {e}")
            except Exception:
                pass
        # Aviso si no se encontró
        try:
            self.append_info("[LOGO] No se encontró imagen en ui/images o assets (ej: ui/images/seg.png).")
        except Exception:
            pass
        return None

    # ---------- Layout ----------
    def create_widgets(self):
        root_frame = ttk.Frame(self.root, padding=0)
        root_frame.grid(row=0, column=0, sticky="nsew")
        root_frame.rowconfigure(1, weight=1)
        root_frame.columnconfigure(0, weight=1)

        # Header
        header = ttk.Frame(root_frame, style="Header.TFrame", padding=(16, 12))
        header.grid(row=0, column=0, sticky="ew")
        header.columnconfigure(0, weight=1)

        ttk.Label(header, text="Lecturas KV2C / KV2A analyzer", style="Header.TLabel").grid(row=0, column=0, sticky="w")
        self.seg_logo_img = self._load_seg_logo()
        if self.seg_logo_img:
            ttk.Label(header, image=self.seg_logo_img, style="Header.TLabel").grid(row=0, column=1, sticky="e")
        else:
            ttk.Label(header, text="SEG", style="Header.TLabel").grid(row=0, column=1, sticky="e")

        # Body: panel izquierdo opciones, derecho log
        body = ttk.Frame(root_frame, padding=12)
        body.grid(row=1, column=0, sticky="nsew")
        body.columnconfigure(0, weight=0)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(0, weight=1)

        # Panel opciones (card)
        opts = ttk.Labelframe(body, text="Opciones", style="Section.TLabelframe")
        opts.grid(row=0, column=0, sticky="ns", padx=(0, 12))
        for i in range(6):
            opts.columnconfigure(i, weight=0)

        # Carpeta
        ttk.Label(opts, text="Carpeta").grid(row=0, column=0, sticky="w")
        self.folder_path = ttk.Entry(opts, width=40)
        self.folder_path.grid(row=0, column=1, columnspan=4, sticky="ew", padx=8)
        ttk.Button(opts, text="Examinar...", command=self.browse_folder).grid(row=0, column=5, sticky="e")

        # Resolución
        ttk.Label(opts, text="Resolución").grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.resolution = ttk.Combobox(opts, values=["15min", "1h"], state="readonly", width=8)
        self.resolution.set("15min")
        self.resolution.grid(row=1, column=1, sticky="w", padx=8, pady=(8, 0))

        # Rango fechas/horas
        ttk.Label(opts, text="Inicio").grid(row=2, column=0, sticky="w", pady=(8, 0))
        self.start_date = DateEntry(opts, date_pattern="dd/mm/y", width=10)
        self.start_date.grid(row=2, column=1, sticky="w", padx=(8, 4), pady=(8, 0))
        self.start_hour = ttk.Spinbox(opts, from_=0, to=23, width=3)
        self.start_hour.set("00")
        self.start_hour.grid(row=2, column=2, sticky="w", pady=(8, 0))
        self.start_min = ttk.Spinbox(opts, from_=0, to=59, width=3)
        self.start_min.set("00")
        self.start_min.grid(row=2, column=3, sticky="w", padx=(4, 0), pady=(8, 0))

        ttk.Label(opts, text="Fin").grid(row=3, column=0, sticky="w")
        self.end_date = DateEntry(opts, date_pattern="dd/mm/y", width=10)
        self.end_date.grid(row=3, column=1, sticky="w", padx=(8, 4))
        self.end_hour = ttk.Spinbox(opts, from_=0, to=23, width=3)
        self.end_hour.set("23")
        self.end_hour.grid(row=3, column=2, sticky="w")
        self.end_min = ttk.Spinbox(opts, from_=0, to=59, width=3)
        self.end_min.set("59")
        self.end_min.grid(row=3, column=3, sticky="w", padx=(4, 0))

        # Botonera
        btns = ttk.Frame(opts)
        btns.grid(row=4, column=0, columnspan=6, sticky="ew", pady=(12, 0))
        btns.columnconfigure(0, weight=1)
        ttk.Button(btns, text="Analizar CSV", style="Accent.TButton", command=self.analyze_folder).grid(row=0, column=0, sticky="ew")
        ttk.Button(btns, text="Analizar PRN", command=self.analyze_folder_prn).grid(row=0, column=1, sticky="ew", padx=(8, 0))
        # Botones de exportación (inician deshabilitados)
        self.export_excel_btn = ttk.Button(btns, text="Exportar Excel", command=self.export_excel, state="disabled")
        self.export_excel_btn.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        self.export_csv_btn = ttk.Button(btns, text="Exportar CSV", command=self.export_csv, state="disabled")
        self.export_csv_btn.grid(row=1, column=1, sticky="ew", padx=(8, 0), pady=(8, 0))
        # Botón limpiar
        self.clear_btn = ttk.Button(btns, text="Limpiar", command=self.clear_results)
        self.clear_btn.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(8, 0))

        # Panel de resultados (log)
        right = ttk.Labelframe(body, text="Registro y resultados", style="Section.TLabelframe")
        right.grid(row=0, column=1, sticky="nsew")
        right.rowconfigure(0, weight=1)
        right.columnconfigure(0, weight=1)

        self.info_text = tk.Text(right, height=20, wrap="word", font=("Consolas", 10))
        yscroll = ttk.Scrollbar(right, orient="vertical", command=self.info_text.yview)
        self.info_text.configure(yscrollcommand=yscroll.set)
        self.info_text.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")

        # Permitir expandir el panel derecho
        body.rowconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)

    def _build_statusbar(self):
        bar = ttk.Frame(self.root, padding=(12, 6))
        bar.grid(row=2, column=0, sticky="ew")
        bar.columnconfigure(0, weight=1)
        self.status_label = ttk.Label(bar, text="Listo")
        self.status_label.grid(row=0, column=0, sticky="w")
        self.progress = ttk.Progressbar(bar, mode="indeterminate", length=160)
        self.progress.grid(row=0, column=1, sticky="e")

    # ---------- Utilidades UI ----------
    def set_busy(self, busy: bool, msg: str = ""):
        if busy:
            self.status_label.config(text=msg or "Procesando…")
            try:
                self.progress.start(12)
            except Exception:
                pass
        else:
            try:
                self.progress.stop()
            except Exception:
                pass
            self.status_label.config(text=msg or "Listo")

    def append_info(self, text):
        self.info_text.configure(state="normal")
        self.info_text.insert("end", text + "\n")
        self.info_text.see("end")
        self.info_text.configure(state="disabled")

    def browse_folder(self):
        folder = filedialog.askdirectory(title="Selecciona carpeta con archivos")
        if folder:
            self.folder_path.delete(0, tk.END)
            self.folder_path.insert(0, folder)

    # ---------- Saneo de horas/minutos ----------
    def _sanitize_time_inputs(self):
        def to_int(v, default):
            try:
                return int(str(v).strip())
            except Exception:
                return default
        def clamp(v, lo, hi):
            return max(lo, min(hi, v))
        sh = clamp(to_int(self.start_hour.get(), 0), 0, 23)
        sm = clamp(to_int(self.start_min.get(), 0), 0, 59)
        eh = clamp(to_int(self.end_hour.get(), 23), 0, 23)
        em = clamp(to_int(self.end_min.get(), 59), 0, 59)
        # Reflejar en la UI por si el usuario puso 60, etc.
        try:
            self.start_hour.set(f"{sh:02d}")
            self.start_min.set(f"{sm:02d}")
            self.end_hour.set(f"{eh:02d}")
            self.end_min.set(f"{em:02d}")
        except Exception:
            pass
        return sh, sm, eh, em

    # ---------- Acciones ----------
    def analyze_folder(self):
        self._run_analysis("csv")

    def analyze_folder_prn(self):
        self._run_analysis("prn")

    def _run_analysis(self, file_type="csv"):
        folder_path = self.folder_path.get()
        if not folder_path or not os.path.exists(folder_path):
            messagebox.showerror("Error", "Selecciona una carpeta válida")
            return

        # Fechas y horas
        sdate = self.start_date.get_date()
        edate = self.end_date.get_date()
        sh, sm, eh, em = self._sanitize_time_inputs()
        start_dt = datetime(sdate.year, sdate.month, sdate.day, sh, sm)
        end_dt = datetime(edate.year, edate.month, edate.day, eh, em)
        if end_dt < start_dt:
            messagebox.showerror("Error", "El fin debe ser posterior al inicio")
            return

        resolution = self.resolution.get()

        # Preparar UI
        self.info_text.configure(state="normal")
        self.info_text.delete("1.0", "end")
        self.info_text.configure(state="disabled")
        self.set_busy(True, "Procesando…")

        def progress_cb(msg: str):
            self.root.after(0, lambda: self.append_info(msg))

        def month_span(s, e):
            y, m = s.year, s.month
            while (y < e.year) or (y == e.year and m <= e.month):
                yield y, m
                m += 1
                if m > 12:
                    m = 1
                    y += 1

        def worker():
            try:
                months = list(month_span(sdate, edate))
                monthly_dfs, all_details = [], []
                total_files = total_processed = total_errors = 0
                last_folder = folder_path

                for (yy, mm) in months:
                    # Límites de hora por mes iterado
                    if (yy, mm) == (sdate.year, sdate.month) and (yy, mm) == (edate.year, edate.month):
                        s_t = f"{sh:02d}:{sm:02d}"
                        e_t = f"{eh:02d}:{em:02d}"
                    elif (yy, mm) == (sdate.year, sdate.month):
                        s_t, e_t = f"{sh:02d}:{sm:02d}", "23:59"
                    elif (yy, mm) == (edate.year, edate.month):
                        s_t, e_t = "00:00", f"{eh:02d}:{em:02d}"
                    else:
                        s_t, e_t = "00:00", "23:59"

                    if file_type == "csv":
                        ok, msg, results = self.csv_processor.analyze_folder(
                            Path(folder_path), mes_usuario=mm, año_usuario=yy,
                            start_time=s_t, end_time=e_t, progress_cb=progress_cb
                        )
                    else:
                        if hasattr(self.csv_processor, "analyze_folder_prn"):
                            ok, msg, results = self.csv_processor.analyze_folder_prn(
                                Path(folder_path), mes_usuario=mm, año_usuario=yy,
                                start_time=s_t, end_time=e_t, progress_cb=progress_cb
                            )
                        else:
                            ok, msg, results = False, "Función PRN no disponible", None

                    if ok and getattr(self.csv_processor, "combined_df", None) is not None:
                        monthly_dfs.append(self.csv_processor.combined_df.copy())
                        all_details.extend(results.get("file_details", []))
                        total_files += results.get("total_files", 0)
                        total_processed += results.get("processed_files", 0)
                        total_errors += results.get("error_files", 0)
                        last_folder = results.get("folder", last_folder)

                if not monthly_dfs:
                    self.root.after(0, lambda: self.set_busy(False, "Sin datos"))
                    self.root.after(0, lambda: self.append_info("No se generaron datos"))
                    return

                combined = pd.concat(monthly_dfs, ignore_index=True)

                # Normalizar timestamp si no es datetime
                if "timestamp" in combined.columns and not pd.api.types.is_datetime64_any_dtype(combined["timestamp"]):
                    # Intentos múltiples de parseo
                    combined["timestamp"] = pd.to_datetime(
                        combined["timestamp"].astype(str).str.strip(),
                        errors="coerce", dayfirst=True
                    )
                # Eliminar filas sin timestamp válido
                if "timestamp" in combined.columns:
                    before_rows = combined.shape[0]
                    combined = combined[pd.notna(combined["timestamp"])].copy()
                    after_parse_rows = combined.shape[0]
                else:
                    before_rows = combined.shape[0]
                    after_parse_rows = before_rows

                # Filtro por rango final
                if "timestamp" in combined.columns:
                    combined = combined[(combined["timestamp"] >= start_dt) & (combined["timestamp"] <= end_dt)]
                after_filter_rows = combined.shape[0]

                if resolution == "1h" and not combined.empty and "timestamp" in combined.columns:
                    combined["hour_ts"] = combined["timestamp"].dt.floor("H")
                    agg_cols = {c: "sum" for c in ["kwh", "kvarh"] if c in combined.columns}
                    grouped = combined.groupby(["company", "hour_ts"], as_index=False).agg(agg_cols)
                    combined = grouped.rename(columns={"hour_ts": "timestamp"})
                    combined = combined.sort_values(["company", "timestamp"]).reset_index(drop=True)

                self.csv_processor.combined_df = combined

                # Agregados y detalles
                fmt = "%d/%m/%y %H:%M"
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
                dedup_details = list(agg.values())
                for a in dedup_details:
                    a["start_date"] = start_dt.strftime(fmt)
                    a["end_date"] = end_dt.strftime(fmt)

                total_files_u = len(dedup_details)
                processed_u = sum(1 for x in dedup_details if x.get("success"))
                error_u = total_files_u - processed_u

                results_agg = {
                    "folder": last_folder,
                    "total_files": total_files_u,
                    "processed_files": processed_u,
                    "error_files": error_u,
                    "date_range": {
                        "start": start_dt.strftime("%d/%m/%y %H:%M"),
                        "end": end_dt.strftime("%d/%m/%y %H:%M")
                    },
                    "combined_stats": {
                        "total_rows": int(combined.shape[0]),
                        "total_columns": int(combined.shape[1]),
                        "total_kwh_values": int(pd.notna(combined["kwh"]).sum()) if "kwh" in combined.columns else 0,
                        "total_kvar_values": int(pd.notna(combined["kvarh"]).sum()) if "kvarh" in combined.columns else 0,
                        "resolution": resolution,
                        "rows_before_parse": before_rows,
                        "rows_after_parse": after_parse_rows,
                        "rows_after_filter": after_filter_rows
                    },
                    "file_details": dedup_details,
                    "errors": []
                }
                self.root.after(0, lambda: self.on_analysis_done(True, f"Procesamiento {file_type.upper()} completado", results_agg))
            except Exception as e:
                self.root.after(0, lambda: self.show_error(str(e)))
            finally:
                self.root.after(0, lambda: self.set_busy(False, "Listo"))

        threading.Thread(target=worker, daemon=True).start()

    def append_info(self, text: str):
        self.info_text.configure(state="normal")
        self.info_text.insert(tk.END, text.rstrip() + "\n")
        self.info_text.see(tk.END)
        self.info_text.configure(state="disabled")

    def clear_results(self):
        """Limpia panel de información y deshabilita exportaciones."""
        self.info_text.configure(state="normal")
        self.info_text.delete("1.0", "end")
        self.info_text.configure(state="disabled")
        self.export_excel_btn.configure(state="disabled")
        self.export_csv_btn.configure(state="disabled")
        self.append_info("Panel limpiado.")
        self.last_results = None
        self.csv_processor.combined_df = None

    def on_analysis_done(self, ok: bool, msg: str, results: dict):
        self.append_info(msg)
        if ok and getattr(self.csv_processor, "combined_df", None) is not None:
            self.last_results = results
            if hasattr(self, "export_excel_btn"):
                self.export_excel_btn.configure(state="normal")
            if hasattr(self, "export_csv_btn"):
                self.export_csv_btn.configure(state="normal")
            cs = results.get("combined_stats", {})
            self.append_info(f"Filas: {cs.get('total_rows', 0)}  Columnas: {cs.get('total_columns', 0)}  Resolución: {cs.get('resolution', '')}")
        else:
            self.append_info("Sin resultados para exportar.")

    def show_error(self, msg: str):
        self.append_info(f"[ERROR] {msg}")
        try:
            messagebox.showerror("Error", msg)
        except Exception:
            pass

    def export_excel(self):
        df = getattr(self.csv_processor, "combined_df", None)
        if df is None or df.empty:
            messagebox.showinfo("Exportar", "No hay datos para exportar.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel", "*.xlsx")],
            title="Guardar como Excel"
        )
        if not path:
            return

        def sanitize_sheet_name(name: str, used: set) -> str:
            base = re.sub(r'[\\/*?:\[\]]', "_", str(name)) or "Hoja"
            base = base[:31]
            # Evitar nombres reservados como 'History' en algunos builds
            if base.strip().lower() in {"history"}:
                base = f"_{base}"
            candidate = base
            i = 1
            while candidate in used or len(candidate) == 0:
                suffix = f"_{i}"
                candidate = (base[: max(0, 31 - len(suffix))] + suffix) or f"Hoja_{i}"
                i += 1
            used.add(candidate)
            return candidate

        def apply_datetime_format(writer, sheet_name: str, df_sheet: pd.DataFrame):
            """Aplica formato dd/mm/yy hh:mm a columnas datetime."""
            try:
                ws = writer.sheets[sheet_name]
                from openpyxl.styles import numbers  # noqa: F401
                for j, col in enumerate(df_sheet.columns, start=1):
                    if pd.api.types.is_datetime64_any_dtype(df_sheet[col]):
                        for col_cells in ws.iter_cols(min_col=j, max_col=j, min_row=2, max_row=ws.max_row):
                            for cell in col_cells:
                                cell.number_format = "dd/mm/yy hh:mm"
            except Exception:
                # Si openpyxl no está, simplemente no se aplica formato
                pass

        try:
            with pd.ExcelWriter(path, engine="openpyxl") as writer:
                used_names = set()
                # 1) Hoja por empresa
                if "company" in df.columns:
                    for comp, grp in df.groupby(df["company"].astype(str), dropna=True):
                        sheet = sanitize_sheet_name(f"{comp}", used_names)
                        grp.to_excel(writer, index=False, sheet_name=sheet)
                        apply_datetime_format(writer, sheet, grp)
                    # 2) Resumen
                    resumen_cols = [c for c in ["kwh", "kvarh"] if c in df.columns]
                    if resumen_cols:
                        resumen = df.groupby(df["company"].astype(str), as_index=False)[resumen_cols].sum()
                    else:
                        resumen = df.groupby(df["company"].astype(str), as_index=False).size().rename(columns={"size": "rows"})
                    resumen_sheet = sanitize_sheet_name("Resumen", used_names)
                    resumen.to_excel(writer, index=False, sheet_name=resumen_sheet)
                # 3) Combinado
                comb_sheet = sanitize_sheet_name("Combinado", used_names)
                df.to_excel(writer, index=False, sheet_name=comb_sheet)
                apply_datetime_format(writer, comb_sheet, df)
                # 4) Detalles
                details = None
                if hasattr(self, "last_results") and isinstance(self.last_results, dict):
                    details = self.last_results.get("file_details")
                if details:
                    try:
                        det_df = pd.DataFrame(details)
                        det_sheet = sanitize_sheet_name("Detalles", used_names)
                        det_df.to_excel(writer, index=False, sheet_name=det_sheet)
                    except Exception:
                        pass
            messagebox.showinfo("Exportar", f"Archivo guardado:\n{path}")
        except Exception as e:
            self.show_error(str(e))

    def export_csv(self):
        df = getattr(self.csv_processor, "combined_df", None)
        if df is None or df.empty:
            messagebox.showinfo("Exportar", "No hay datos para exportar.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv")],
            title="Guardar como CSV"
        )
        if not path:
            return
        try:
            out = df.copy()
            if "timestamp" in out.columns and pd.api.types.is_datetime64_any_dtype(out["timestamp"]):
                out["timestamp"] = out["timestamp"].dt.strftime("%d/%m/%y %H:%M")
            # Si existen otras columnas datetime y quieres formatearlas también:
            # for c in out.select_dtypes(include=["datetime64[ns]"]).columns:
            #     out[c] = out[c].dt.strftime("%d/%m/%y %H:%M")
            out.to_csv(path, index=False, encoding="utf-8-sig")
            messagebox.showinfo("Exportar", f"Archivo guardado:\n{path}")
        except Exception as e:
            self.show_error(str(e))

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