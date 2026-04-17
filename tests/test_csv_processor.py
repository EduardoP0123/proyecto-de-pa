# ...existing code...
import pytest
from pathlib import Path
from src.csv_processor import CSVProcessor

def test_init(tmp_path):
    p = tmp_path / "ws"
    proc = CSVProcessor(p)
    assert proc.workspace == p
    assert proc.combined_df is None

def test_clean_numeric_column():
    import pandas as pd
    proc = CSVProcessor()
    s = pd.Series(["1.23", " 4,56 ", "abc", "7.89"])
    result = proc._clean_numeric_column(s)
    assert result.notna().sum() == 3
    assert abs(result.iloc[0] - 1.23) < 1e-6
    assert abs(result.iloc[1] - 4.56) < 1e-6
    assert abs(result.iloc[3] - 7.89) < 1e-6
