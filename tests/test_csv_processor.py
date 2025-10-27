# ...existing code...
import pytest
from pathlib import Path
from src.csv_processor import CSVProcessor

def test_init(tmp_path):
    p = tmp_path / "ws"
    proc = CSVProcessor(p)
    assert proc.workspace.exists()
    assert proc.input_dir.exists()
    assert proc.output_dir.exists()
