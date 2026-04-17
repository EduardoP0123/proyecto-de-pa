from pathlib import Path
from datetime import datetime
from typing import Optional
import pandas as pd
import re

from src.csv_processor import parse_datetime_series

class PRNProcessor:
    """Procesa archivos .PRN produciendo un DataFrame con columnas
    company, timestamp, kwh, kvarh (cuando se puedan inferir).
    No modifica nada de la lógica existente de CSV; se usa en paralelo.
    """
    def __init__(self):
        self.last_df = None
        self.last_details = []

    def _read_prn(self, path: Path, preferred_month: Optional[int] = None) -> pd.DataFrame:
        """Lee un PRN que puede NO tener encabezado.
        Formato ejemplo (sin header):
        "TELEPUERTO    " "08/01/25" "00:15" 15 0.124050 0.009600
        -> company, date, time, interval, kwh, kvarh
        Si tiene encabezado normal se usa directamente.
        """
        # Leer primeras líneas para detectar si hay header
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            first_line = f.readline().strip()
        # Heurística: si la primera línea contiene múltiples campos entre comillas y números finales => no header
        no_header_pattern = re.compile(r'".+"\s+"\d{1,2}/\d{1,2}/\d{2,4}"\s+"\d{1,2}:\d{2}"')
        has_no_header = bool(no_header_pattern.search(first_line))

        if has_no_header:
            df = pd.read_csv(
                path,
                header=None,
                quotechar='"',
                sep=r"\s+",
                engine='python'
            )
            # Esperado >=6 columnas; asignar nombres base
            while df.shape[1] < 6:
                # A veces pandas puede juntar columnas si separadores irregulares; relectura con fallback simple
                df = pd.read_csv(path, header=None, quotechar='"', sep=r"\s+", engine='python')
                break
            cols = df.columns.tolist()
            # Asignar nombres
            name_map = {}
            if len(cols) >= 6:
                name_map = {cols[0]: 'company_raw', cols[1]: 'date_raw', cols[2]: 'time_raw', cols[3]: 'interval', cols[4]: 'kwh_raw', cols[5]: 'kvarh_raw'}
            else:
                # Fallback genérico
                for i, c in enumerate(cols):
                    name_map[c] = f'col_{i}'
            df.rename(columns=name_map, inplace=True)
            # Construir campos normalizados
            df['company'] = df.get('company_raw', '').astype(str).str.replace('"', '', regex=False).str.strip()
            # Parse fecha+hora (intenta primero MM/DD/YY luego DD/MM/YY)
            date_series = df.get('date_raw', '').astype(str).str.replace('"', '', regex=False).str.strip()
            time_series = df.get('time_raw', '').astype(str).str.replace('"', '', regex=False).str.strip()
            df['timestamp'] = parse_datetime_series(date_series, time_series, preferred_month=preferred_month)
            # Limpiar energía
            kwh_vals = pd.to_numeric(df.get('kwh_raw', ''), errors='coerce')
            kvar_vals = pd.to_numeric(df.get('kvarh_raw', ''), errors='coerce')
            df['kwh'] = kwh_vals
            df['kvarh'] = kvar_vals
            # Seleccionar columnas finales
            out = df[['company', 'timestamp', 'kwh', 'kvarh']].copy()
            out = out.dropna(subset=['timestamp'])
            return out.reset_index(drop=True)
        else:
            # Intento con header presente
            try:
                df = pd.read_csv(path, sep=None, engine='python', header=0)
            except Exception:
                df = pd.read_csv(path, sep=r"\s+", engine='python', header=0)
            df.columns = [str(c).strip() for c in df.columns]
            return df

    def _detect_timestamp(self, df: pd.DataFrame, preferred_month: Optional[int] = None) -> pd.Series:
        cols = list(df.columns)
        lower_map = {c: c.lower() for c in cols}
        # 1. Nombres comunes
        date_col = next((c for c in cols if any(k in lower_map[c] for k in ("fecha","date"))), None)
        time_col = next((c for c in cols if any(k in lower_map[c] for k in ("hora","time"))), None)
        if date_col:
            if time_col:
                ts = parse_datetime_series(df[date_col].astype(str), df[time_col].astype(str), preferred_month=preferred_month)
            else:
                ts = parse_datetime_series(df[date_col].astype(str), preferred_month=preferred_month)
            if ts.notna().sum() > 0:
                return ts
        # 2. Regex heurística
        date_re = re.compile(r"(\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|\d{4}[/-]\d{1,2}[/-]\d{1,2})")
        time_re = re.compile(r"\b\d{1,2}:\d{2}(:\d{2})?\b")
        d_candidate = None; t_candidate = None
        for c in cols:
            sample = df[c].dropna().astype(str).str.strip().head(50)
            ratio = sample.apply(lambda x: bool(date_re.fullmatch(x))).mean()
            if ratio > 0.3:
                d_candidate = c; break
        for c in cols:
            sample = df[c].dropna().astype(str).str.strip().head(50)
            ratio = sample.apply(lambda x: bool(time_re.fullmatch(x))).mean()
            if ratio > 0.3:
                t_candidate = c; break
        if d_candidate:
            if t_candidate:
                ts = parse_datetime_series(df[d_candidate].astype(str), df[t_candidate].astype(str), preferred_month=preferred_month)
            else:
                ts = parse_datetime_series(df[d_candidate].astype(str), preferred_month=preferred_month)
            if ts.notna().sum() > 0:
                return ts
        # 3. Fallback: primera columna
        first = cols[0] if cols else None
        if first:
            ts = parse_datetime_series(df[first].astype(str), preferred_month=preferred_month)
            if ts.notna().sum() > 0:
                return ts
        return pd.Series([pd.NaT]*len(df))

    def _select_energy_columns(self, df: pd.DataFrame):
        # Busca nombres directos
        kwh = next((c for c in df.columns if "kwh" in c.lower() and "kvar" not in c.lower()), None)
        kvar = next((c for c in df.columns if "kvar" in c.lower()), None)
        if kwh or kvar:
            return kwh, kvar
        # Alternativas semánticas
        kwh_alt = next((c for c in df.columns if c.lower() in ("energia activa","activa","kw.h")), None)
        kvar_alt = next((c for c in df.columns if c.lower() in ("energia reactiva","reactiva","kvar.h")), None)
        return kwh_alt, kvar_alt

    def _clean_num(self, s: pd.Series) -> pd.Series:
        s = (s.astype(str).str.replace("\xa0"," ", regex=False)
               .str.replace(",",".", regex=False)
               .str.strip())
        return pd.to_numeric(s, errors="coerce")

    def load_folder(self, folder: Path, mes_usuario: int, año_usuario: int,
                    start_time: str, end_time: str, progress_cb=None):
        prn_files = list(folder.glob("*.prn"))
        details = []
        frames = []
        h_start = datetime.strptime(start_time, "%H:%M").time()
        h_end = datetime.strptime(end_time, "%H:%M").time()
        for f in prn_files:
            try:
                df = self._read_prn(f, preferred_month=mes_usuario)
                # Si el parser ya devolvió el formato final (company,timestamp,kwh,kvarh)
                if set(['company','timestamp']).issubset(df.columns) and 'kwh' in df.columns:
                    pass
                else:
                    # Aplicar detección legacy
                    ts = self._detect_timestamp(df, preferred_month=mes_usuario)
                    df['timestamp'] = ts
                df = df.dropna(subset=['timestamp'])
                if df.empty:
                    details.append({"filename": f.name, "rows": 0, "success": False, "error": "sin timestamp"})
                    if progress_cb: progress_cb(f"PRN sin timestamp: {f.name}")
                    continue
                # Filtrar mes y rango hora
                df = df[(df["timestamp"].dt.year == año_usuario) & (df["timestamp"].dt.month == mes_usuario)]
                if df.empty:
                    details.append({"filename": f.name, "rows": 0, "success": True})
                    continue
                df = df[(df["timestamp"].dt.time >= h_start) & (df["timestamp"].dt.time <= h_end)]
                if df.empty:
                    details.append({"filename": f.name, "rows": 0, "success": True})
                    continue
                if {'kwh','kvarh','company'}.issubset(df.columns):
                    out = df[['company','timestamp','kwh','kvarh']].copy()
                    # Normalizar company si viene crudo
                    out['company'] = out['company'].astype(str).str.strip().str.replace('"','', regex=False)
                else:
                    kwh_col, kvar_col = self._select_energy_columns(df)
                    kwh_vals = self._clean_num(df[kwh_col]) if kwh_col else pd.Series([pd.NA]*len(df))
                    kvar_vals = self._clean_num(df[kvar_col]) if kvar_col else pd.Series([pd.NA]*len(df))
                    out = pd.DataFrame({
                        "company": f.stem,
                        "timestamp": df["timestamp"],
                        "kwh": kwh_vals,
                        "kvarh": kvar_vals
                    })
                frames.append(out)
                details.append({
                    "filename": f.name,
                    "rows": int(out.shape[0]),
                    "success": True,
                    "kwh_values": int(pd.notna(out["kwh"]).sum()),
                    "kvar_values": int(pd.notna(out["kvarh"]).sum()),
                })
                if progress_cb: progress_cb(f"PRN procesado: {f.name} ({out.shape[0]} filas)")
            except Exception as e:
                details.append({"filename": f.name, "rows": 0, "success": False, "error": str(e)})
                if progress_cb: progress_cb(f"Error PRN: {f.name} - {e}")
        if frames:
            df_all = pd.concat(frames, ignore_index=True).sort_values(["company","timestamp"]).reset_index(drop=True)
        else:
            df_all = pd.DataFrame(columns=["company","timestamp","kwh","kvarh"])
        self.last_df = df_all
        self.last_details = details
        return df_all, details
