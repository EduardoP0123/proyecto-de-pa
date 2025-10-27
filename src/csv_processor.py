"""
CSV processor modular:
- detecta columna de fecha
- carga CSVs tolerando metadatos iniciales
- filtra por mes y año (usuario elige mes/año)
- rellena huecos con 0s
- genera Excel multi-hoja (las sheets ya funcionan)
"""
import re
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, Tuple, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import warnings

import pandas as pd

from .utils import parse_datetime_series, normalize_am_pm, to_numeric

LOG = logging.getLogger(__name__)

class CSVProcessor:
    def __init__(self, workspace: Path = None):
        self.workspace = workspace
        self.combined_df: Optional[pd.DataFrame] = None

    def load_csv(self, path: Path) -> pd.DataFrame:
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        date_line_re = re.compile(r"^\s*\d+\s*,\s*\d{1,2}/\d{1,2}/\d{4}\s")
        fallback_re = re.compile(r"\d{1,2}/\d{1,2}/\d{4}|\d{4}-\d{2}-\d{2}")

        for enc in encodings:
            try:
                with open(path, "r", encoding=enc, errors="ignore") as f:
                    lines = f.readlines()
                first_data_idx = None
                for i, line in enumerate(lines):
                    if date_line_re.search(line):
                        first_data_idx = i
                        break
                if first_data_idx is None:
                    for i, line in enumerate(lines):
                        if fallback_re.search(line) and line.count(",") >= 3:
                            first_data_idx = i
                            break
                if first_data_idx is None:
                    first_data_idx = 0
                df = pd.read_csv(path, encoding=enc, header=None, skiprows=list(range(first_data_idx)))
                df = df.dropna(how="all")
                df.columns = [f"c{i}" for i in range(df.shape[1])]
                return df
            except Exception:
                continue
        raise ValueError(f"No se pudo cargar el archivo: {path}")

    def detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        candidates = [c for c in df.columns if any(k in str(c).lower() for k in ["date", "time", "timestamp", "fecha", "hora", "datetime"])]
        for c in candidates:
            s = parse_datetime_series(df[c].astype(str))
            if s.notna().mean() >= 0.7:
                return c
        best_col = None
        best_ratio = 0.0
        for c in df.columns:
            s = parse_datetime_series(df[c].astype(str))
            ratio = s.notna().mean()
            if ratio > best_ratio:
                best_ratio = ratio
                best_col = c
        return best_col if best_ratio >= 0.5 else None

    def _detect_energy_columns(self, df: pd.DataFrame, ts_col: str) -> Tuple[Optional[str], Optional[str]]:
        kwh_score = {}
        kvar_score = {}
        def s_name(name: str) -> str:
            return str(name).lower().strip()
        for c in df.columns:
            if c == ts_col:
                continue
            n = s_name(c)
            if any(k in n for k in ["kwh"]):
                kwh_score[c] = kwh_score.get(c, 0) + 100
            if any(k in n for k in ["kvah", "apparent"]):
                kwh_score[c] = kwh_score.get(c, 0) + 40
            if any(k in n for k in ["channel 1"]):
                kwh_score[c] = kwh_score.get(c, 0) + 30
            if any(k in n for k in ["kvarh"]):
                kvar_score[c] = kvar_score.get(c, 0) + 100
            if any(k in n for k in ["reactive"]):
                kvar_score[c] = kvar_score.get(c, 0) + 60
            if any(k in n for k in ["channel 2"]):
                kvar_score[c] = kvar_score.get(c, 0) + 30
        kwh_col = max(kwh_score, key=kwh_score.get) if kwh_score else None
        kvar_col = max(kvar_score, key=kvar_score.get) if kvar_score else None
        if kwh_col is None or kvar_col is None:
            numeric_cols = []
            for c in df.columns:
                if c == ts_col:
                    continue
                series_num = to_numeric(df[c])
                if series_num.notna().any():
                    numeric_cols.append((c, series_num.abs().sum(), series_num))
            numeric_cols.sort(key=lambda x: x[1], reverse=True)
            if kwh_col is None and numeric_cols:
                kwh_col = numeric_cols[0][0]
            if kvar_col is None and len(numeric_cols) > 1:
                for c, _, _ in numeric_cols[1:]:
                    if c != kwh_col:
                        kvar_col = c
                        break
        return kwh_col, kvar_col

    def _infer_interval_minutes(self, ts: pd.Series) -> int:
        """Inferir intervalo en minutos (default 15)."""
        if ts.empty:
            return 15
        s = ts.sort_values().drop_duplicates()
        diffs = s.diff().dropna().dt.total_seconds() / 60.0
        if diffs.empty:
            return 15
        mode = int(round(diffs.value_counts().idxmax()))
        return mode if mode in (5, 10, 15, 20, 30, 60) else 15

    def analyze_folder(
        self,
        folder_path: Path,
        mes_usuario=None,
        año_usuario=None,
        start_time: str = "00:00",
        end_time: str = "00:15",
        progress_cb=None
    ):
        """Saca el mes completo por archivo, incluyendo intervalos sin dato (vacíos)."""
        def report(msg: str):
            if progress_cb:
                try:
                    progress_cb(msg)
                except Exception:
                    pass

        csv_files = list(Path(folder_path).glob("*.csv"))
        if not csv_files:
            return False, "No se encontraron archivos CSV en la carpeta", None

        processed, details, errors = [], [], []
        total_rows = 0
        report(f"Archivos detectados: {len(csv_files)}")

        # Construir rango del mes seleccionado (incluye 00:15 del mes siguiente)
        if not (mes_usuario and año_usuario):
            return False, "Debes seleccionar mes y año", None
        sh, sm = map(int, start_time.split(":"))
        eh, em = map(int, end_time.split(":"))
        start_dt = datetime(año_usuario, mes_usuario, 1, sh, sm, 0)
        next_month = 1 if mes_usuario == 12 else mes_usuario + 1
        next_year = año_usuario + 1 if mes_usuario == 12 else año_usuario
        end_dt = datetime(next_year, next_month, 1, eh, em, 0)

        for idx, csv in enumerate(csv_files, start=1):
            report(f"[{idx}/{len(csv_files)}] Procesando {csv.name}")
            try:
                raw = self.load_csv(csv)
                date_col = self.detect_date_column(raw)
                if not date_col:
                    # aunque no haya columna fecha, igual generar rejilla vacía
                    freq_min = 15
                    full_range = pd.date_range(start_dt, end_dt, freq=f"{freq_min}min", inclusive="both")
                    out = pd.DataFrame({
                        "company": csv.stem,
                        "timestamp": full_range,
                        "kwh": "",
                        "kvarh": ""
                    })
                    processed.append(out)
                    total_rows += len(out)
                    details.append({
                        "filename": csv.name, "rows": len(out),
                        "start_date": out["timestamp"].min().strftime("%d/%m/%Y %H:%M"),
                        "end_date": out["timestamp"].max().strftime("%d/%m/%Y %H:%M"),
                        "company": csv.stem, "success": True, "note": "sin fecha en origen"
                    })
                    continue

                # Normalizar/parsear fechas
                raw[date_col] = raw[date_col].astype(str).map(normalize_am_pm)
                raw[date_col] = parse_datetime_series(raw[date_col])
                raw = raw.dropna(subset=[date_col])

                # Detectar columnas de energía SIN convertir a número
                kwh_col, kvar_col = self._detect_energy_columns(raw, date_col)

                # Renombrar y ordenar
                raw = raw.rename(columns={date_col: "timestamp"}).sort_values("timestamp")

                # Inferir intervalo y construir rango completo del mes
                freq_min = self._infer_interval_minutes(raw["timestamp"])
                full_range = pd.date_range(start_dt, end_dt, freq=f"{freq_min}min", inclusive="both")

                # Filtrar solo registros del rango y reindexar a rejilla completa
                in_month = raw[(raw["timestamp"] >= full_range.min()) & (raw["timestamp"] <= full_range.max())].copy()
                in_month = in_month.set_index("timestamp")

                # Series de texto (conservar vacío si no hay dato)
                kwh_series = in_month[kwh_col].astype(str) if kwh_col in in_month.columns else pd.Series(dtype="object")
                kvar_series = in_month[kvar_col].astype(str) if (kvar_col and (kvar_col in in_month.columns)) else pd.Series(dtype="object")

                # Reindexar a toda la rejilla y rellenar con vacío
                kwh_full = kwh_series.reindex(full_range).fillna("")
                kvar_full = kvar_series.reindex(full_range).fillna("")

                out = pd.DataFrame({
                    "company": csv.stem,
                    "timestamp": full_range,
                    "kwh": kwh_full.values,
                    "kvarh": kvar_full.values
                })

                processed.append(out)
                total_rows += len(out)
                details.append({
                    "filename": csv.name,
                    "rows": len(out),
                    "start_date": out["timestamp"].min().strftime("%d/%m/%Y %H:%M"),
                    "end_date": out["timestamp"].max().strftime("%d/%m/%Y %H:%M"),
                    "company": csv.stem,
                    "success": True
                })
                report(f"  ✓ {csv.name}: {len(out)} filas")
            except Exception as e:
                errors.append({'filename': csv.name, 'error': str(e)})
                details.append({"filename": csv.name, "success": False, "error": str(e)})
                report(f"  ✗ {csv.name}: {e}")

        if not processed:
            err = "\n".join([f"- {e['filename']}: {e['error']}" for e in errors]) or "Sin detalles"
            return False, f"No se pudieron procesar archivos CSV\n{err}", None

        combined = pd.concat(processed, ignore_index=True).sort_values(["company", "timestamp"])
        self.combined_df = combined

        results = {
            "folder": str(folder_path),
            "total_files": len(csv_files),
            "processed_files": len(processed),
            "error_files": len(errors),
            "date_range": {
                "start": combined["timestamp"].min().strftime("%d/%m/%Y %H:%M"),
                "end": combined["timestamp"].max().strftime("%d/%m/%Y %H:%M"),
            },
            "combined_stats": {
                "total_rows": int(total_rows),
                "total_columns": int(combined.shape[1]),
                "gaps_filled": 0
            },
            "file_details": details,
            "errors": errors
        }
        report(f"✓ Completado: {len(processed)} archivos, {total_rows} filas")
        return True, f"Procesamiento completado: {len(processed)} archivos procesados", results

    def export_excel_multi_sheet(self, filename: str):
        if self.combined_df is None:
            return False, "No hay datos procesados para exportar"
        try:
            df = self.combined_df.copy()
            df["timestamp"] = df["timestamp"].dt.strftime("%d/%m/%Y %H:%M:%S")
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                for company in df["company"].unique():
                    df[df["company"] == company].to_excel(writer, sheet_name=str(company)[:31], index=False)
                df.to_excel(writer, sheet_name="RESUMEN_COMBINADO", index=False)
            return True, f"Excel exportado: {filename}"
        except Exception as e:
            return False, f"Error exportando Excel: {e}"

    def export_combined_csv(self, filename: str):
        if self.combined_df is None:
            return False, "No hay datos procesados para exportar"
        try:
            df = self.combined_df.copy()
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv(filename, index=False, encoding="utf-8-sig")
            return True, f"CSV combinado exportado: {filename}"
        except Exception as e:
            return False, f"Error exportando CSV: {e}"

    def clear_data(self):
        self.combined_df = None