"""
CSV processor para archivos KV2C - VERSIÓN ARREGLADA
- Lee TODOS los datos sin perder valores
- Detección robusta de columnas kWh/kvarh
- Mantiene TODOS los valores válidos
"""
from pathlib import Path
from typing import Optional, Tuple, List
import logging
import pandas as pd
from datetime import datetime
import re


# Logger simple (si ya tienes otro, puedes reemplazarlo)
LOG = logging.getLogger("csv_processor")
if not LOG.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    LOG.addHandler(handler)
LOG.setLevel(logging.INFO)


# Stubs utilitarios para evitar NameError (ajusta si ya existen en otro módulo)
def normalize_am_pm(value: str) -> str:
    """
    Recibe un solo valor (string) de hora o fecha+hora y si contiene AM/PM lo convierte a 24h.
    Devuelve el valor original si no se puede convertir.
    Acepta variantes como:
      10/31/2025 12:15 AM
      10/31/2025 1:05 PM
      12:30 AM
    """
    if value is None:
        return value
    txt = str(value).strip()
    if not txt:
        return txt
    upper = txt.upper()
    # Casos que ya están en 24h (contienen HH:MM y no AM/PM)
    if ("AM" not in upper and "PM" not in upper):
        return txt
    try:
        # Primero solo hora AM/PM
        dt = datetime.strptime(upper, "%I:%M %p")
        return dt.strftime("%H:%M")
    except Exception:
        pass
    # Intentar fecha + hora
    for fmt in ("%m/%d/%Y %I:%M %p", "%d/%m/%Y %I:%M %p"):
        try:
            dt = datetime.strptime(upper, fmt)
            # Devuelve misma fecha + hora 24h
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            continue
    return txt


def parse_datetime_series(date_series: pd.Series,
                          time_series: Optional[pd.Series] = None,
                          dayfirst: bool = True) -> pd.Series:
    """
    Combina series de fecha y hora en timestamp.
    Retorna NaT donde no fue posible.
    """
    if time_series is not None:
        combo = date_series.astype(str).str.strip() + " " + time_series.astype(str).str.strip()
    else:
        combo = date_series.astype(str).str.strip()
    ts = pd.to_datetime(combo, errors="coerce", dayfirst=dayfirst)
    return ts


class CSVProcessor:
    def __init__(self, workspace: Path = None):
        self.workspace = workspace
        self.combined_df = None

    # ---------------- HEADER KV2C CORRECTO (evitar Scale Factor) ----------------
    def _find_kv2c_header_index(self, lines: list[int | str]) -> int:
        """
        Encuentra la fila de encabezados real de KV2C.
        Preferir: 'Read Date Time','Channel 1','Channel 2','Status Flags'
        Penalizar: '(Scale Factor)'
        """
        best_idx, best_score = 0, -10_000
        for i, raw in enumerate(lines[:200]):
            line = raw.lower()
            # Debe lucir como encabezado con comas suficientes
            if line.count(",") < 4:
                continue
            score = 0
            score += 3 if "read date time" in line else 0
            score += 2 if "channel 1" in line else 0
            score += 2 if "channel 2" in line else 0
            score += 1 if "status flags" in line else 0
            score -= 4 if "scale factor" in line else 0  # penaliza encabezados de factor
            # Fuerte preferencia a la fila que tenga ambos canales sin factor
            if ("channel 1" in line and "channel 2" in line) and ("scale factor" not in line):
                score += 6
            if score > best_score:
                best_idx, best_score = i, score

        # Si el mejor contiene 'scale factor', intenta buscar hacia arriba una versión sin él
        if "scale factor" in str(lines[best_idx]).lower():
            for j in range(max(0, best_idx - 5), best_idx):
                l = str(lines[j]).lower()
                if ("read date time" in l and "channel 1" in l and "channel 2" in l and "scale factor" not in l):
                    return j
        return best_idx

    def load_csv(self, path: Path) -> pd.DataFrame:
        """Carga CSV KV2C detectando el encabezado correcto. Sin low_memory."""
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        last_err = None

        for enc in encodings:
            try:
                with open(path, "r", encoding=enc, errors="ignore") as f:
                    lines = f.readlines()

                hdr_idx = self._find_kv2c_header_index(lines)

                def _read_at(idx: int, engine: str | None = None) -> pd.DataFrame:
                    kwargs = dict(
                        filepath_or_buffer=path,
                        encoding=enc,
                        skiprows=idx,
                        header=0,
                    )
                    if engine:
                        kwargs["engine"] = engine
                    try:
                        # pandas >= 1.3
                        df = pd.read_csv(on_bad_lines="skip", **kwargs)
                    except TypeError:
                        # pandas viejos no tienen on_bad_lines
                        df = pd.read_csv(**kwargs)
                    df.columns = df.columns.str.strip()
                    df = df.loc[:, ~df.columns.str.match(r"^Unnamed", na=False)]
                    df = df.dropna(how="all")
                    return df

                # 1) Intento con engine por defecto (C)
                try:
                    df = _read_at(hdr_idx, engine=None)
                except Exception:
                    # 2) Fallback robusto con engine='python' (SIN low_memory)
                    df = _read_at(hdr_idx, engine="python")

                LOG.info(f"Archivo cargado: {path.name}, header en línea {hdr_idx}, columnas: {list(df.columns)}")
                return df

            except Exception as e:
                last_err = e
                LOG.debug(f"load_csv fallo con {enc}: {e}")
                continue

        raise ValueError(f"No se pudo cargar el archivo: {path} ({last_err})")

    def detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Detecta la columna de fecha/hora."""
        date_keywords = ["read date time", "date time", "datetime", "timestamp", "fecha", "hora"]
        
        # Buscar por nombre
        for keyword in date_keywords:
            for col in df.columns:
                if keyword.lower() in str(col).lower():
                    LOG.info(f"Columna de fecha detectada por nombre: {col}")
                    return col
        
        # Buscar por contenido
        for col in df.columns:
            try:
                sample = df[col].dropna().head(20).astype(str)
                date_pattern = re.compile(r'\d{1,2}/\d{1,2}/\d{4}')
                matches = sample.str.contains(date_pattern, regex=True).sum()
                if matches >= 10:
                    LOG.info(f"Columna de fecha detectada por contenido: {col}")
                    return col
            except:
                continue
        
        LOG.warning("No se encontró columna de fecha")
        return None

    def _detect_energy_columns(self, df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
        """
        Detecta columnas kWh y kvarh de forma agresiva.
        Prioridad:
        1. Columnas con 'kWh' y 'kvarh' en el nombre
        2. Channel 1 y Channel 2
        3. Primeras dos columnas numéricas
        """
        kwh_col = None
        kvar_col = None
        
        # 1. Buscar por nombre exacto
        for col in df.columns:
            col_lower = str(col).lower().replace(" ", "")
            if not kwh_col and "kwh" in col_lower and "kvarh" not in col_lower:
                kwh_col = col
            if not kvar_col and "kvarh" in col_lower:
                kvar_col = col
        
        LOG.info(f"Detección por nombre - kWh: {kwh_col}, kvarh: {kvar_col}")
        
        # 2. Buscar Channel 1/2
        if not kwh_col or not kvar_col:
            for col in df.columns:
                col_lower = str(col).lower()
                if not kwh_col and "channel 1" in col_lower:
                    kwh_col = col
                if not kvar_col and "channel 2" in col_lower:
                    kvar_col = col
        
        LOG.info(f"Detección por Channel - kWh: {kwh_col}, kvarh: {kvar_col}")
        
        # 3. Buscar columnas numéricas
        if not kwh_col or not kvar_col:
            numeric_cols = []
            for col in df.columns:
                try:
                    # Intentar convertir a numérico
                    test = pd.to_numeric(
                        df[col].astype(str)
                        .str.replace("\xa0", " ")
                        .str.replace(" ", "")
                        .str.replace(",", ".")
                        .str.strip(),
                        errors='coerce'
                    )
                    # Si tiene al menos 50% de valores numéricos válidos
                    if test.notna().mean() > 0.5:
                        numeric_cols.append(col)
                except:
                    continue
            
            if not kwh_col and len(numeric_cols) > 0:
                kwh_col = numeric_cols[0]
            if not kvar_col and len(numeric_cols) > 1:
                kvar_col = numeric_cols[1]
        
        LOG.info(f"Detección final - kWh: {kwh_col}, kvarh: {kvar_col}")
        return kwh_col, kvar_col

    def _clean_numeric(self, series: pd.Series) -> pd.Series:
        """Limpia y convierte columna a numérico, preservando todos los valores válidos."""
        if series is None or series.empty:
            return pd.Series(dtype="float64")
        
        # Convertir a string y limpiar
        cleaned = (series.astype(str)
                   .str.replace("\xa0", " ", regex=False)
                   .str.replace(" ", "", regex=False)
                   .str.replace(",", ".", regex=False)
                   .str.strip())
        
        # Convertir a numérico
        numeric = pd.to_numeric(cleaned, errors='coerce')
        
        valid_count = numeric.notna().sum()
        LOG.info(f"Limpieza numérica: {valid_count}/{len(series)} valores válidos")
        
        return numeric

    # ==================== KV DETECCIÓN Y LIMPIEZA ====================

    def _clean_numeric_column(self, series: pd.Series) -> pd.Series:
        s = (series.astype(str).str.strip()
             .str.replace("\xa0", " ", regex=False)
             .str.replace(" ", "", regex=False)
             .str.replace(",", ".", regex=False)
             .str.replace(r"[^0-9.\-]", "", regex=True))
        return pd.to_numeric(s, errors="coerce")

    def _kv_name_candidates(self, df: pd.DataFrame):
        """Preferir Channel 1/Channel 2 reales; excluir Scale Factor/Status."""
        kwh, kvar = [], []
        for col in df.columns:
            cl = str(col).lower()
            if ("channel 1" in cl) and ("scale factor" not in cl) and ("status" not in cl) and ("flag" not in cl):
                kwh.append(col)
            if ("channel 2" in cl) and ("scale factor" not in cl) and ("status" not in cl) and ("flag" not in cl):
                kvar.append(col)
            cl_ns = cl.replace(" ", "")
            if "kwh" in cl_ns and "kvar" not in cl_ns:
                if col not in kwh:
                    kwh.append(col)
            if "kvarh" in cl_ns or ("kvar" in cl_ns and "kwh" not in cl_ns):
                if col not in kvar:
                    kvar.append(col)
        return kwh, kvar

    def _kv_numeric_candidates(self, df: pd.DataFrame):
        """
        Candidatos numéricos generales (EXCLUYE solo fechas/flags).
        OJO: NO excluimos 'Scale Factor' porque en tus archivos
        'Channel X (Scale Factor)' contiene los valores de energía.
        """
        cands = []
        skip = ("set number", "common flags", "status flags",
                "read date time", "date time", "fecha", "hora", "date", "time", "timestamp")
        for col in df.columns:
            cl = str(col).lower()
            if any(x in cl for x in skip):
                continue
            sample = df[col].dropna().head(100)
            if sample.empty:
                continue
            if self._clean_numeric_column(sample).notna().mean() >= 0.7:
                cands.append(col)
        return cands

    def _select_best_energy_pair(self, df: pd.DataFrame):
        """
        Devuelve (kwh_col, kvar_col, kwh_series_float, kvar_series_float)
        probando pares y eligiendo el que tenga más valores válidos.
        """
        def clean(col):
            return self._clean_numeric_column(df[col]) if col in df.columns else pd.Series(dtype="float64", index=df.index)

        best_score = -1
        best = (None, None, pd.Series(dtype="float64", index=df.index), pd.Series(dtype="float64", index=df.index))

        kwh_names, kvar_names = self._kv_name_candidates(df)
        pairs = []
        if kwh_names and kvar_names:
            for kc in kwh_names:
                for qc in kvar_names:
                    pairs.append((kc, qc))
        else:
            nums = self._kv_numeric_candidates(df)
            for i in range(len(nums)):
                for j in range(i + 1, len(nums)):
                    pairs.append((nums[i], nums[j]))

        # Si aún no hay pares, intenta explícitamente Channel 1/2 por último recurso
        if not pairs:
            ch1 = next((c for c in df.columns if "channel 1" in str(c).lower() or "channel1" in str(c).lower()), None)
            ch2 = next((c for c in df.columns if "channel 2" in str(c).lower() or "channel2" in str(c).lower()), None)
            if ch1 and ch2:
                pairs.append((ch1, ch2))

        for kc, qc in pairs:
            k = clean(kc)
            q = clean(qc)
            score = k.notna().sum() + q.notna().sum()
            if score > best_score:
                best_score = score
                best = (kc, qc, k, q)

        return best

    def _aggregate_energy(self, df: pd.DataFrame, ts_col: str) -> pd.DataFrame:
        """
        Consolida kWh/kvarh por timestamp sin perder datos.
        - Usa Channel 1 → kWh y Channel 2 → kvarh cuando existan.
        - Si hay múltiples columnas/filas por timestamp, toma el valor máximo válido.
        """
        kwh_names, kvar_names = self._kv_name_candidates(df)

        def stack_and_agg(col_list, new_col):
            frames = []
            for c in col_list or []:
                if c in df.columns:
                    s = self._clean_numeric_column(df[c])
                    frames.append(pd.DataFrame({ts_col: df[ts_col], new_col: s}))
            if not frames:
                return pd.DataFrame(columns=[ts_col, new_col])
            long = pd.concat(frames, ignore_index=True)
            return long.groupby(ts_col, as_index=False)[new_col].max()

        kwh_agg = stack_and_agg(kwh_names, "kwh_val")
        kvar_agg = stack_and_agg(kvar_names, "kvar_val")

        # Fallback robusto: escoger mejor par si falta alguno
        if kwh_agg.empty or kvar_agg.empty:
            kc, qc, ks, qs = self._select_best_energy_pair(df)
            if kwh_agg.empty and kc is not None:
                kwh_agg = pd.DataFrame({ts_col: df[ts_col], "kwh_val": ks}).groupby(ts_col, as_index=False)["kwh_val"].max()
            if kvar_agg.empty and qc is not None:
                kvar_agg = pd.DataFrame({ts_col: df[ts_col], "kvar_val": qs}).groupby(ts_col, as_index=False)["kvar_val"].max()

        out = pd.merge(kwh_agg, kvar_agg, on=ts_col, how="outer")
        return out


    def export_excel_multi_sheet(self, filename: str):
        """Exporta a Excel con una hoja por empresa + resumen combinado"""
        if self.combined_df is None:
            return False, "No hay datos procesados para exportar"
        try:
            df = self.combined_df.copy()
            df["timestamp"] = df["timestamp"].dt.strftime("%d/%m/%Y %H:%M:%S")
            
            with pd.ExcelWriter(filename, engine="openpyxl") as writer:
                for company in df["company"].unique():
                    company_df = df[df["company"] == company].copy()
                    sheet_name = str(company)[:31]
                    company_df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                df.to_excel(writer, sheet_name="RESUMEN_COMBINADO", index=False)
            
            return True, f"Excel exportado: {filename}"
        except Exception as e:
            return False, f"Error exportando Excel: {e}"

    def export_combined_csv(self, filename: str):
        """Exporta a CSV combinado (formato ISO para fechas)"""
        if self.combined_df is None:
            return False, "No hay datos procesados"
        try:
            df = self.combined_df.copy()
            df["timestamp"] = df["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df.to_csv(filename, index=False, encoding="utf-8-sig")
            return True, f"CSV exportado: {filename}"
        except Exception as e:
            return False, f"Error: {e}"

    def clear_data(self):
        self.combined_df = None

    def analyze_folder(
        self,
        folder_path: Path,
        mes_usuario=None,
        año_usuario=None,
        start_time: str = "00:00",
        end_time: str = "00:15",
        progress_cb=None
    ):
        """
        Procesa todos los CSV en folder_path y construye 'company,timestamp,kwh,kvarh'.
        No modifica la lógica de FECHAS del UI; aquí solo parseamos para poder agrupar.
        """
        def report(msg: str):
            if progress_cb:
                try:
                    progress_cb(msg)
                except Exception:
                    pass

        csv_files = list(Path(folder_path).glob("*.csv"))
        if not csv_files:
            return False, "No se encontraron archivos CSV en la carpeta", None

        if not (mes_usuario and año_usuario):
            return False, "Debes seleccionar mes y año", None

        # Ventana del mes (NO cambiar lógica de fechas)
        sh, sm = map(int, start_time.split(":"))
        eh, em = map(int, end_time.split(":"))
        start_dt = datetime(año_usuario, mes_usuario, 1, sh, sm, 0)
        next_month = 1 if mes_usuario == 12 else mes_usuario + 1
        next_year = año_usuario + 1 if mes_usuario == 12 else año_usuario
        end_dt = datetime(next_year, next_month, 1, eh, em, 0)
        # Generar rejilla completa 15 min entre inicio y fin detectados del mes
        full_range = pd.date_range(start_dt, end_dt, freq="15min", inclusive="both")
        # Calcular filas esperadas según largo del mes (sin forzar un número fijo)
        import calendar
        dias_mes = calendar.monthrange(start_dt.year, start_dt.month)[1]
        expected_rows = dias_mes * 24 * 4  # 96 intervalos por día
        # Si falta último tramo, extender hasta final real del mes
        if len(full_range) < expected_rows:
            last_minute = datetime(start_dt.year, start_dt.month, dias_mes, 23, 45)
            full_range = pd.date_range(start_dt, last_minute, freq="15min")
            LOG.info(f"Rejilla extendida a {len(full_range)} filas (mes de {dias_mes} días).")
        elif len(full_range) > expected_rows:
            full_range = full_range[:expected_rows]
            LOG.info(f"Rejilla recortada a {expected_rows} filas (mes de {dias_mes} días).")
        start_str = full_range.min().strftime("%d/%m/%Y %H:%M")
        end_str = full_range.max().strftime("%d/%m/%Y %H:%M")

        processed = []
        details = []
        errors = []

        report(f"Archivos detectados: {len(csv_files)}")

        for i, csv_path in enumerate(csv_files, start=1):
            report(f"[{i}/{len(csv_files)}] Procesando {csv_path.name}")
            try:
                df = self.load_csv(csv_path)

                # Detectar columna fecha sin cambiar tu lógica global
                date_col = self.detect_date_column(df)
                if not date_col:
                    # Rejilla vacía si no hay fecha
                    out = pd.DataFrame({
                        "company": csv_path.stem,
                        "timestamp": full_range,
                        "kwh": pd.NA,
                        "kvarh": pd.NA
                    })
                    processed.append(out)
                    details.append({
                        "filename": csv_path.name,
                        "rows": len(out),
                        "success": True,
                        "note": "sin fecha",
                        "start_date": start_str,
                        "end_date": end_str
                    })
                    continue

                # Parseo local para poder agrupar; no toca tu UI
                date_series = df[date_col].astype(str).apply(normalize_am_pm)
                ts = parse_datetime_series(date_series)
                df = df.copy()
                df["__ts__"] = ts
                df = df.dropna(subset=["__ts__"])
                if df.empty:
                    details.append({"filename": csv_path.name, "rows": 0, "success": False, "error": "fechas inválidas"})
                    continue

                # Consolidar energía por timestamp (usa helpers ya añadidos)
                energy = self._aggregate_energy(df, "__ts__")

                # Filtrar al rango y reindexar a rejilla completa
                in_month = energy[(energy["__ts__"] >= full_range.min()) &
                                  (energy["__ts__"] <= full_range.max())].copy()
                if not in_month.empty:
                    in_month = in_month.set_index("__ts__")
                    kwh_full = in_month["kwh_val"].reindex(full_range)
                    kvar_full = in_month["kvar_val"].reindex(full_range)
                else:
                    kwh_full = pd.Series(index=full_range, dtype="float64")
                    kvar_full = pd.Series(index=full_range, dtype="float64")

                final_df = pd.DataFrame({
                    "company": csv_path.stem,
                    "timestamp": full_range,
                    "kwh": kwh_full.values,
                    "kvarh": kvar_full.values
                })

                processed.append(final_df)
                details.append({
                    "filename": csv_path.name,
                    "rows": len(final_df),
                    "success": True,
                    "kwh_values": int(pd.notna(final_df["kwh"]).sum()),
                    "kvar_values": int(pd.notna(final_df["kvarh"]).sum()),
                    "start_date": start_str,
                    "end_date": end_str
                })

            except Exception as e:
                LOG.exception(f"Error procesando {csv_path.name}")
                errors.append({"filename": csv_path.name, "error": str(e)})
                details.append({
                    "filename": csv_path.name,
                    "rows": 0,
                    "success": False,
                    "error": str(e),
                    "start_date": start_str,
                    "end_date": end_str
                })

        if not processed:
            err = "\n".join([f"- {e['filename']}: {e['error']}" for e in errors]) or "Sin detalles"
            return False, f"No se procesaron archivos\n{err}", None

        combined = pd.concat(processed, ignore_index=True).sort_values(["company", "timestamp"])
        self.combined_df = combined

        results = {
            "folder": str(folder_path),
            "total_files": len(csv_files),
            "processed_files": len(processed),
            "error_files": len(errors),
            "date_range": {
                "start": start_str,
                "end": end_str,
            },
            "combined_stats": {
                "total_rows": int(combined.shape[0]),
                "total_columns": int(combined.shape[1]),
                "total_kwh_values": int(pd.notna(combined["kwh"]).sum()),
                "total_kvar_values": int(pd.notna(combined["kvarh"]).sum()),
            },
            "file_details": details,
            "errors": errors
        }
        return True, f"Procesamiento completado: {len(processed)} archivos procesados", results

    def load_prn(self, path: Path) -> pd.DataFrame:
        """
        Intenta leer un archivo PRN (generalmente separado por espacios o tabulaciones).
        Se limpia encabezado y normaliza nombres.
        """
        try:
            df = pd.read_csv(path, sep=None, engine="python", header=0)
        except Exception:
            # Fallback a whitespace
            df = pd.read_csv(path, delim_whitespace=True, header=0)
        df.columns = [c.strip().lower() for c in df.columns]
        # Normalizar columnas esperadas si existen
        for maybe in ["fecha", "date"]:
            if maybe in df.columns and "timestamp" not in df.columns:
                # Intento parsear fecha y hora si hay columna hora
                if "hora" in df.columns:
                    df["timestamp"] = pd.to_datetime(df[maybe] + " " + df["hora"], errors="coerce", dayfirst=True)
                else:
                    df["timestamp"] = pd.to_datetime(df[maybe], errors="coerce", dayfirst=True)
        if "timestamp" not in df.columns:
            # Busca combinaciones
            for f_col in ["date", "fecha"]:
                for h_col in ["time", "hora"]:
                    if f_col in df.columns and h_col in df.columns:
                        df["timestamp"] = pd.to_datetime(df[f_col] + " " + df[h_col], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["timestamp"])
        df = df.sort_values("timestamp")
        df.reset_index(drop=True, inplace=True)
        return df

    def analyze_folder_prn(self, folder: Path, mes_usuario: int, año_usuario: int,
                           start_time: str, end_time: str, progress_cb=None):
        """
        Similar a analyze_folder para CSV pero filtrando archivos .prn
        (Reutiliza atributos combined_df y exportaciones).
        """
        prn_files = list(folder.glob("*.prn"))
        details = []
        total_ok = 0
        for f in prn_files:
            try:
                df = self.load_prn(f)
                # Filtro por mes/año y rango de hora dentro del mes
                df = df[(df["timestamp"].dt.year == año_usuario) & (df["timestamp"].dt.month == mes_usuario)]
                if not df.empty:
                    h_start = datetime.strptime(start_time, "%H:%M").time()
                    h_end   = datetime.strptime(end_time, "%H:%M").time()
                    df = df[(df["timestamp"].dt.time >= h_start) & (df["timestamp"].dt.time <= h_end)]
                if df.empty:
                    details.append({"filename": f.name, "rows": 0, "success": True,
                                    "kwh_values": 0, "kvar_values": 0})
                    continue
                # Intentar detectar columnas de energía
                kwh_col = next((c for c in df.columns if "kwh" in c), None)
                kvar_col = next((c for c in df.columns if "kvar" in c), None)
                if kwh_col and "kwh" not in df.columns:
                    df["kwh"] = pd.to_numeric(df[kwh_col], errors="coerce")
                if kvar_col and "kvarh" not in df.columns:
                    df["kvarh"] = pd.to_numeric(df[kvar_col], errors="coerce")

                df["company"] = f.stem  # etiqueta simple
                self.combined_df = df if self.combined_df is None else pd.concat([self.combined_df, df], ignore_index=True)
                details.append({
                    "filename": f.name,
                    "rows": int(df.shape[0]),
                    "success": True,
                    "kwh_values": int(df["kwh"].notna().sum()) if "kwh" in df.columns else 0,
                    "kvar_values": int(df["kvarh"].notna().sum()) if "kvarh" in df.columns else 0
                })
                total_ok += 1
                if progress_cb:
                    progress_cb(f"PRN procesado: {f.name} ({df.shape[0]} filas)")
            except Exception as e:
                details.append({"filename": f.name, "rows": 0, "success": False, "error": str(e),
                                "kwh_values": 0, "kvar_values": 0})
                if progress_cb:
                    progress_cb(f"Error PRN: {f.name} - {e}")

        if self.combined_df is not None:
            self.combined_df.sort_values(["company", "timestamp"], inplace=True)
            self.combined_df.reset_index(drop=True, inplace=True)

        return True, "PRN analizado", {
            "folder": str(folder),
            "total_files": len(prn_files),
            "processed_files": total_ok,
            "error_files": len(prn_files) - total_ok,
            "file_details": details
        }