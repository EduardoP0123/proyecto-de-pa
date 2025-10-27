from typing import Optional
from datetime import datetime
import pandas as pd

DATE_FORMATS = [
    "%d/%m/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%Y-%m-%d %H:%M:%S",
    "%d/%m/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
    "%d/%m/%Y %I:%M %p",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%d %H:%M",
]

def normalize_am_pm(s: str) -> str:
    """
    Normaliza AM/PM en español: 'a. m.', 'p. m.', 'a.m.', NBSP, etc. → 'AM'/'PM'
    """
    if not isinstance(s, str):
        return s
    s = s.replace("\xa0", " ")  # NBSP → space
    repl = {
        " a. m.": " AM", " p. m.": " PM",
        "a. m.": "AM",  "p. m.": "PM",
        "a.m.": "AM",   "p.m.": "PM",
        " a. m": " AM", " p. m": " PM",
        "a. m": "AM",   "p. m": "PM",
        " a.m.": " AM", " p.m.": " PM",
        " a. m.": " AM"," p. m.": " PM",  # con NBSP
        "a. m.": "AM",  "p. m.": "PM",
    }
    for k, v in repl.items():
        s = s.replace(k, v)
    return s

def try_parse_datetime(value):
    if pd.isna(value):
        return pd.NaT
    text = normalize_am_pm(str(value)).strip()
    for fmt in DATE_FORMATS:
        try:
            return pd.to_datetime(text, format=fmt, errors="raise")
        except Exception:
            continue
    # fallback con dayfirst=True para cultura es-*
    return pd.to_datetime(text, errors="coerce", dayfirst=True)

def parse_datetime_series(series: pd.Series) -> pd.Series:
    return series.apply(try_parse_datetime)

def to_numeric(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.replace("\u00A0", " ").str.strip()
    # números con coma decimal
    s = s.str.replace(".", "", regex=False) if s.str.contains(r"\d\.\d{3}", regex=True).any() else s
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")
