"""
UI wrapper que no se tocará por petición del usuario.
Este módulo exporta una función `run_ui()` que la UI existente debe invocar.
"""
from pathlib import Path
from .csv_processor import CSVProcessor

def run_ui(workspace: Path):
    return CSVProcessor(workspace)
