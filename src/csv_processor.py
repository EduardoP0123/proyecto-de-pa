"""
CSV processor para archivos KV2C
- Mantiene precisión completa de decimales (sin redondeos forzados).
- Extiende el límite del rango hasta las 00:00 del mes siguiente.
- Detección robusta de columnas y limpieza estricta.
"""
from pathlib import Path
from typing import Optional, Tuple, List
import logging
import pandas as pd
from datetime import datetime
import re
import warnings

LOG = logging.getLogger("csv_processor")
if not LOG.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    LOG.addHandler(handler)
LOG.setLevel(logging.INFO)

def normalize_am_pm(value: str) -> str:
    if value is None:
        return value
    txt = str(value).strip()
    if not txt:
        return txt
    normalized = txt.replace("\xa0", " ")
    repl = {
        " a. m.": " AM", " p. m.": " PM",
        "a. m.": "AM", "p. m.": "PM",
        "a.m.": "AM", "p.m.": "PM",
        " a. m": " AM", " p. m": " PM",
        "a. m": "AM", "p. m": "PM",
        " a.m.": " AM", " p.m.": " PM",
        " a.ám.": " AM", " p.ám.": " PM",
        "a.ám.": "AM", "p.ám.": "PM",
    }
    low = normalized.lower()
    for k, v in repl.items():
        low = low.replace(k, v.lower())
    txt = low.upper().strip()

    upper = txt.upper()
    if ("AM" not in upper and "PM" not in upper):
        return txt
    try:
        dt = datetime.strptime(upper, "%I:%M %p")
        return dt.strftime("%H:%M")
    except Exception:
        pass
    for fmt in ("%m/%d/%Y %I:%M %p", "%d/%m/%Y %I:%M %p"):
        try:
            dt = datetime.strptime(upper, fmt)
            return dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            continue
    return txt

def parse_datetime_series(date_series: pd.Series,
                          time_series: Optional[pd.Series] = None,
                          preferred_month: Optional[int] = None) -> pd.Series:
    base = date_series.astype(str).str.strip()
    if time_series is not None:
        combos = (base + " " + time_series.astype(str).str.strip()).fillna("")
    else:
        combos = base.fillna("")
    combos = combos.str.strip().apply(normalize_am_pm)
    if combos.empty:
        return pd.Series([pd.NaT] * len(date_series), index=date_series.index)
    lower = combos.str.lower()
    combos_clean = combos.copy().astype(object)
    combos_clean = combos_clean.where(combos != "", None)
    combos_clean = combos_clean.where(lower != "nan", None)

    attempts = []

    def try_format(fmt: Optional[str] = None, dayfirst: Optional[bool] = None):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                ts = pd.to_datetime(combos_clean, errors="coerce", format=fmt, dayfirst=dayfirst)
            except Exception:
                ts = pd.Series(pd.NaT, index=combos.index)
        
        score = ts.notna().sum()
        month_score = 0
        if preferred_month and score > 0:
            try:
                month_score = int((ts.dt.month == preferred_month).sum())
            except Exception:
                month_score = 0
        attempts.append((month_score, score, ts))

    fmt_candidates = [
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%d/%m/%y %H:%M:%S", "%d/%m/%y %H:%M",
        "%m/%d/%y %H:%M:%S", "%m/%d/%y %H:%M", "%d/%m/%Y %I:%M:%S %p", "%d/%m/%Y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %I:%M %p", "%d/%m/%y %I:%M:%S %p", "%d/%m/%y %I:%M %p",
        "%m/%d/%y %I:%M:%S %p", "%m/%d/%y %I:%M %p",
    ]
    has_ampm = combos.str.contains("AM", case=False, na=False) | combos.str.contains("PM", case=False, na=False)
    for fmt in fmt_candidates:
        if "%p" not in fmt or has_ampm.any():
            try_format(fmt=fmt)

    try_format(dayfirst=True)
    try_format(dayfirst=False)

    if preferred_month:
        best = max(attempts, key=lambda item: (item[0], item[1]), default=(0, 0, pd.Series(pd.NaT, index=combos.index)))
    else:
        best = max(attempts, key=lambda item: item[1], default=(0, 0, pd.Series(pd.NaT, index=combos.index)))
    ts = best[2]
    if ts.index is not combos.index:
        ts = ts.reindex(combos.index)
    return ts

class CSVProcessor:
    def __init__(self, workspace: Path = None):
        self.workspace = workspace
        self.combined_df = None
        self._last_file_metadata = {}

    def _extract_meter_metadata(self, lines: list, hdr_idx: int) -> dict:
        metadata = {"meter_id": None, "scale_factor": 1.0}
        for line in lines[:hdr_idx]:
            low = line.lower().strip()
            if low.startswith("meter id") or low.startswith("meter_id"):
                parts = re.split(r'[,\t]', line, maxsplit=1)
                if len(parts) > 1:
                    val = parts[1].strip().strip('"').strip()
                    if val:
                        metadata["meter_id"] = val
                        break
        if hdr_idx + 1 < len(lines):
            subhdr = lines[hdr_idx + 1]
            matches = re.findall(r'\((\d+(?:\.\d+)?)\)', subhdr)
            for m_str in matches:
                try:
                    sf = float(m_str)
                    if sf > 1:
                        metadata["scale_factor"] = sf
                        break
                except Exception:
                    pass
        return metadata

    def _find_kv2c_header_index(self, lines: list[int | str]) -> int:
        best_idx, best_score = 0, -10_000
        for i, raw in enumerate(lines[:200]):
            line = raw.lower()
            if line.count(",") < 4:
                continue
            score = 0
            score += 3 if "read date time" in line else 0
            score += 2 if "channel 1" in line else 0
            score += 2 if "channel 2" in line else 0
            score += 1 if "status flags" in line else 0
            score -= 4 if "scale factor" in line else 0
            if ("channel 1" in line and "channel 2" in line) and ("scale factor" not in line):
                score += 6
            if score > best_score:
                best_idx, best_score = i, score

        if "scale factor" in str(lines[best_idx]).lower():
            for j in range(max(0, best_idx - 5), best_idx):
                l = str(lines[j]).lower()
                if ("read date time" in l and "channel 1" in l and "channel 2" in l and "scale factor" not in l):
                    return j
        return best_idx

    def load_csv(self, path: Path) -> pd.DataFrame:
        encodings = ["utf-8-sig", "cp1252", "latin1"]
        last_err = None

        for enc in encodings:
            try:
                with open(path, "r", encoding=enc, errors="ignore") as f:
                    lines = f.readlines()

                hdr_idx = self._find_kv2c_header_index(lines)

                # Extract meter metadata before reading the DataFrame
                metadata = self._extract_meter_metadata(lines, hdr_idx)
                self._last_file_metadata = metadata

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
                        df = pd.read_csv(on_bad_lines="skip", **kwargs)
                    except TypeError:
                        df = pd.read_csv(**kwargs)
                    df.columns = df.columns.str.strip()
                    df = df.loc[:, ~df.columns.str.match(r"^Unnamed", na=False)]
                    df = df.dropna(how="all")
                    return df

                try:
                    df = _read_at(hdr_idx, engine=None)
                except Exception:
                    df = _read_at(hdr_idx, engine="python")

                LOG.info(f"Archivo cargado: {path.name}, header en línea {hdr_idx}, columnas: {list(df.columns)}")
                return df

            except Exception as e:
                last_err = e
                LOG.debug(f"load_csv fallo con {enc}: {e}")
                continue

        raise ValueError(f"No se pudo cargar el archivo: {path} ({last_err})")

    def detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        date_keywords = ["read date time", "date time", "datetime", "timestamp", "fecha", "hora"]
        
        for keyword in date_keywords:
            for col in df.columns:
                if keyword.lower() in str(col).lower():
                    LOG.info(f"Columna de fecha detectada por nombre: {col}")
                    return col
        
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

    def _clean_numeric_column(self, series: pd.Series) -> pd.Series:
        s = series.astype(str).str.strip()
        s = s.str.replace("\xa0", " ", regex=False)
        s = s.str.replace(" ", "", regex=False)
        s = s.str.replace(",", "", regex=False)
        s = s.str.replace(r"[^0-9.\-]", "", regex=True)
        return pd.to_numeric(s, errors="coerce")

    def _kv_name_candidates(self, df: pd.DataFrame):
        kwh, kvar = [], []
        exclusions = ["status", "flag", "common", "set number", "date", "time", "fecha", "hora", "cumulative", "total"]
        
        for col in df.columns:
            cl = str(col).lower()
            if any(exc in cl for exc in exclusions):
                continue
            if "channel 1" in cl:
                kwh.append(col)
            elif "channel 2" in cl:
                kvar.append(col)
            elif "kwh" in cl and "kvar" not in cl:
                kwh.append(col)
            elif "kvarh" in cl or ("kvar" in cl and "kwh" not in cl):
                kvar.append(col)
        return kwh, kvar

    def _kv_numeric_candidates(self, df: pd.DataFrame):
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
        kwh_names, kvar_names = self._kv_name_candidates(df)

        def pick_best_column(col_list):
            best_col = None
            best_score = -1
            for c in col_list or []:
                if c not in df.columns:
                    continue
                s = self._clean_numeric_column(df[c])
                non_null = int(s.notna().sum())
                non_zero = int((s.fillna(0) != 0).sum())
                score = (non_null * 10) + non_zero
                if score > best_score:
                    best_score = score
                    best_col = c
            return best_col

        best_kwh_col = pick_best_column(kwh_names)
        best_kvar_col = pick_best_column(kvar_names)

        def stack_and_agg(col_list, new_col):
            frames = []
            for c in col_list or []:
                if c in df.columns:
                    s = self._clean_numeric_column(df[c])
                    frames.append(pd.DataFrame({ts_col: df[ts_col], new_col: s}))
            if not frames:
                return pd.DataFrame(columns=[ts_col, new_col])
            long = pd.concat(frames, ignore_index=True)
            long = long.dropna(subset=[new_col])
            return long.groupby(ts_col, as_index=False)[new_col].first()

        kwh_agg = stack_and_agg([best_kwh_col] if best_kwh_col else [], "kwh_val")
        kvar_agg = stack_and_agg([best_kvar_col] if best_kvar_col else [], "kvar_val")

        if kwh_agg.empty or kvar_agg.empty:
            kc, qc, ks, qs = self._select_best_energy_pair(df)
            if kwh_agg.empty and kc is not None:
                kwh_agg = (
                    pd.DataFrame({ts_col: df[ts_col], "kwh_val": ks})
                    .dropna(subset=["kwh_val"])
                    .groupby(ts_col, as_index=False)["kwh_val"]
                    .first()
                )
            if kvar_agg.empty and qc is not None:
                kvar_agg = (
                    pd.DataFrame({ts_col: df[ts_col], "kvar_val": qs})
                    .dropna(subset=["kvar_val"])
                    .groupby(ts_col, as_index=False)["kvar_val"]
                    .first()
                )

        out = pd.merge(kwh_agg, kvar_agg, on=ts_col, how="outer")
        return out

    def export_excel_multi_sheet(self, filename: str):
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

        import calendar
        sh, sm = map(int, start_time.split(":"))
        eh, em = map(int, end_time.split(":"))
        dias_mes = calendar.monthrange(año_usuario, mes_usuario)[1]
        start_dt = datetime(año_usuario, mes_usuario, 1, sh, sm, 0)

        end_dt = datetime(año_usuario, mes_usuario, dias_mes, eh, em, 0)
        if eh == 23 and em == 59:
            end_dt = end_dt + pd.Timedelta(minutes=1)

        # kV2C timestamps are end-of-interval: the 00:00 reading on day 1
        # covers Jul 31 23:45–Aug 1 00:00 and belongs to the PREVIOUS month.
        # First valid interval of the billing month starts at 00:15.
        grid_start = start_dt + pd.Timedelta(minutes=15) if (sh == 0 and sm == 0) else start_dt
        full_range = pd.date_range(grid_start, end_dt, freq="15min")
        expected_rows = len(full_range)
        LOG.info(f"Rejilla generada: {expected_rows} filas.")
        start_str = full_range.min().strftime("%d/%m/%Y %H:%M")
        end_str = full_range.max().strftime("%d/%m/%Y %H:%M")

        processed = []
        details = []
        errors = []
        scale_factors = {}

        report(f"Archivos detectados: {len(csv_files)}")

        for i, csv_path in enumerate(csv_files, start=1):
            report(f"[{i}/{len(csv_files)}] Procesando {csv_path.name}")
            try:
                df = self.load_csv(csv_path)

                # Resolve company name and scale factor from metadata
                metadata = getattr(self, '_last_file_metadata', {})
                raw_meter_id = metadata.get('meter_id')
                if raw_meter_id:
                    company_name = str(raw_meter_id).strip().strip('"').strip("'").strip()
                else:
                    company_name = csv_path.stem
                scale_factor = metadata.get('scale_factor', 1.0)

                date_col = self.detect_date_column(df)
                if not date_col:
                    out = pd.DataFrame({
                        "company": company_name,
                        "timestamp": full_range,
                        "kwh": pd.NA,
                        "kvarh": pd.NA
                    })
                    processed.append(out)
                    file_detail = {
                        "filename": csv_path.name,
                        "rows": len(out),
                        "success": True,
                        "note": "sin fecha",
                        "start_date": start_str,
                        "end_date": end_str
                    }
                    if scale_factor > 1:
                        file_detail["scale_factor"] = scale_factor
                    details.append(file_detail)
                    if scale_factor > 1:
                        scale_factors[company_name] = scale_factor
                    continue

                date_series = df[date_col].astype(str).apply(normalize_am_pm)
                ts = parse_datetime_series(date_series, preferred_month=mes_usuario)
                df = df.copy()
                df["__ts__"] = ts
                df = df.dropna(subset=["__ts__"])
                if df.empty:
                    details.append({"filename": csv_path.name, "rows": 0, "success": False, "error": "fechas inválidas"})
                    continue

                energy = self._aggregate_energy(df, "__ts__")

                kwh_valid = energy["kwh_val"].notna().sum() if "kwh_val" in energy.columns else 0
                kvar_valid = energy["kvar_val"].notna().sum() if "kvar_val" in energy.columns else 0
                if kwh_valid == 0 and kvar_valid == 0:
                    report(f"  ⚠ {csv_path.name}: 0 valores de energía detectados (revisar encabezados)")

                if "kwh_val" in energy.columns:
                    neg_count = (energy["kwh_val"].dropna() < 0).sum()
                    if neg_count > 0:
                        report(f"  ⚠ {csv_path.name}: {neg_count} valores kWh negativos encontrados")
                        energy.loc[energy["kwh_val"] < 0, "kwh_val"] = 0.0

                    outlier_mask = energy["kwh_val"] > 1000
                    outlier_count = outlier_mask.sum()
                    if outlier_count > 0:
                        report(f"  ⚠ {csv_path.name}: {outlier_count} valores anómalos (>1000 kWh/15m) eliminados para proteger el cálculo de kW.")
                        energy.loc[outlier_mask, "kwh_val"] = pd.NA

                if "kvar_val" in energy.columns:
                    neg_count = (energy["kvar_val"].dropna() < 0).sum()
                    if neg_count > 0:
                        report(f"  ⚠ {csv_path.name}: {neg_count} valores kVArh negativos encontrados")
                        energy.loc[energy["kvar_val"] < 0, "kvar_val"] = 0.0

                dup_ts = energy["__ts__"].duplicated().sum()
                if dup_ts > 0:
                    report(f"  ⚠ {csv_path.name}: {dup_ts} timestamps duplicados (se conserva primero)")
                    energy = energy.drop_duplicates(subset=["__ts__"], keep="first")

                in_month = energy[(energy["__ts__"] >= full_range.min()) &
                                  (energy["__ts__"] <= full_range.max())].copy()

                if not in_month.empty:
                    in_month = in_month.set_index("__ts__")
                    kwh_full = in_month["kwh_val"]
                    kvar_full = in_month["kvar_val"]
                    full_range_actual = in_month.index
                else:
                    kwh_full = pd.Series(dtype="float64")
                    kvar_full = pd.Series(dtype="float64")
                    full_range_actual = []

                final_df = pd.DataFrame({
                    "company": company_name,
                    "timestamp": full_range_actual,
                    "kwh": kwh_full.values,
                    "kvarh": kvar_full.values
                })

                processed.append(final_df)
                file_detail = {
                    "filename": csv_path.name,
                    "rows": len(final_df),
                    "success": True,
                    "kwh_values": int(pd.notna(final_df["kwh"]).sum()),
                    "kvar_values": int(pd.notna(final_df["kvarh"]).sum()),
                    "start_date": start_str,
                    "end_date": end_str
                }
                if scale_factor > 1:
                    file_detail["scale_factor"] = scale_factor
                    scale_factors[company_name] = scale_factor
                details.append(file_detail)

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
            "scale_factors": scale_factors,
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