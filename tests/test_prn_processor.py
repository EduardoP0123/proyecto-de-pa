import tempfile
from pathlib import Path
import pandas as pd
from src.prn_processor import PRNProcessor

SAMPLE_NO_HEADER = '"TELEPUERTO" "08/01/25" "00:15" 15 0.124050 0.009600\n"TELEPUERTO" "08/01/25" "00:30" 15 0.100000 0.008000\n'

SAMPLE_WITH_HEADER = 'Fecha Hora kWh kVArh Empresa\n08/01/2025 00:15 0.124050 0.009600 TELEPUERTO\n08/01/2025 00:30 0.100000 0.008000 TELEPUERTO\n'

def write_temp_prn(content: str) -> Path:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix='.prn')
    tmp.write(content.encode('utf-8'))
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


def test_no_header_prn_parses_rows():
    p = PRNProcessor()
    f = write_temp_prn(SAMPLE_NO_HEADER)
    # Fecha en sample es 08/01/25 -> agosto 1 de 2025 (MM/DD/YY)
    df, details = p.load_folder(f.parent, mes_usuario=8, año_usuario=2025, start_time='00:00', end_time='23:59')
    # Filter to our temp file stem
    df = df[df['company'].str.contains('TELEPUERTO')]
    assert not df.empty
    assert set(['company','timestamp','kwh','kvarh']).issubset(df.columns)
    # Check timestamp and values (august)
    assert pd.to_datetime('2025-08-01 00:15:00') in set(df['timestamp'])
    assert abs(df.iloc[0]['kwh'] - 0.124050) < 1e-6


def test_header_prn_parses_rows():
    p = PRNProcessor()
    f = write_temp_prn(SAMPLE_WITH_HEADER)
    # Usa formato 08/01/2025 00:15 (DD/MM/YYYY) -> enero
    df, details = p.load_folder(f.parent, mes_usuario=1, año_usuario=2025, start_time='00:00', end_time='23:59')
    assert not df.empty
    assert set(['company','timestamp','kwh','kvarh']).issubset(df.columns)
