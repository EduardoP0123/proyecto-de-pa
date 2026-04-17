"""
Diagnóstico detallado de las discrepancias en KWh, KVARh, KW
Ejecutar para identificar exactamente dónde están los errores de cálculo.
"""
from pathlib import Path
import pandas as pd
import sys

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent))
from src.csv_processor import CSVProcessor

# Configuración - AJUSTAR según tus datos
INPUT_FOLDERS = [
    Path(r"C:/Users/User/Downloads/lecturas"),
    Path(r"C:/Users/User/Downloads/jul"),
    Path(r"C:/Users/User/Downloads/Load Profile ZFSI Mes de Agosto 2025"),
]
TEST_MONTH = 9  # Mes a probar
TEST_YEAR = 2025
MULTIPLO = 80  # Factor de transformación


def analyze_single_file(csv_path: Path, month: int, year: int):
    """Analiza un solo archivo y muestra diagnóstico detallado."""
    print(f"\n{'='*60}")
    print(f"ARCHIVO: {csv_path.name}")
    print(f"{'='*60}")
    
    proc = CSVProcessor()
    df = proc.load_csv(csv_path)
    
    # Mostrar columnas
    print(f"\nColumnas detectadas ({len(df.columns)}):")
    for i, col in enumerate(df.columns):
        sample_vals = df[col].dropna().head(3).tolist()
        print(f"  [{i}] '{col}': {sample_vals[:3]}")
    
    # Detectar fecha
    date_col = proc.detect_date_column(df)
    print(f"\nColumna de fecha: {date_col}")
    
    # Verificar candidatos kWh/kvarh
    kwh_names, kvar_names = proc._kv_name_candidates(df)
    print(f"\nCandidatos kWh: {kwh_names}")
    print(f"Candidatos kVArh: {kvar_names}")
    
    # Analizar cada columna candidata
    print("\n--- Análisis de columnas numéricas ---")
    for col in kwh_names + kvar_names:
        if col in df.columns:
            s = proc._clean_numeric_column(df[col])
            non_null = s.notna().sum()
            non_zero = (s.fillna(0) != 0).sum()
            total = len(s)
            print(f"  '{col[:40]}': {non_null}/{total} válidos, {non_zero} no-cero, "
                  f"sum={s.sum():.4f}, mean={s.mean():.6f}, max={s.max():.6f}")
    
    return df


def run_full_analysis(folder: Path, month: int, year: int, multiplo: float):
    """Ejecuta análisis completo de una carpeta."""
    print(f"\n{'#'*70}")
    print(f"# PROCESANDO CARPETA: {folder}")
    print(f"# Mes={month}, Año={year}, Multiplo={multiplo}")
    print(f"{'#'*70}")
    
    if not folder.exists():
        print(f"  ERROR: carpeta no existe")
        return
    
    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        print(f"  ERROR: no hay archivos CSV")
        return
    
    print(f"\nArchivos encontrados: {len(csv_files)}")
    
    # Analizar el primer archivo en detalle
    if csv_files:
        analyze_single_file(csv_files[0], month, year)
    
    # Procesar con CSVProcessor
    proc = CSVProcessor()
    ok, msg, results = proc.analyze_folder(
        folder, 
        mes_usuario=month, 
        año_usuario=year, 
        start_time="00:00", 
        end_time="23:59"
    )
    
    if not ok:
        print(f"\n  ERROR en procesamiento: {msg}")
        return
    
    df = proc.combined_df
    if df is None or df.empty:
        print("  ERROR: DataFrame vacío")
        return
    
    print(f"\n{'='*60}")
    print("RESULTADOS DEL PROCESAMIENTO")
    print(f"{'='*60}")
    
    # Estadísticas por empresa
    summary = df.groupby("company").agg(
        rows=("timestamp", "count"),
        kwh_sum=("kwh", "sum"),
        kwh_mean=("kwh", "mean"),
        kwh_max=("kwh", "max"),
        kwh_min=("kwh", "min"),
        kwh_nulls=("kwh", lambda x: x.isna().sum()),
        kvarh_sum=("kvarh", "sum"),
        kvarh_max=("kvarh", "max"),
        kvarh_nulls=("kvarh", lambda x: x.isna().sum()),
    )
    
    print("\nPor empresa (valores BASE sin multiplicador):")
    print(summary.to_string())
    
    # Cálculos con múltiplo
    print(f"\n{'='*60}")
    print(f"CÁLCULOS CON MULTIPLO = {multiplo}")
    print(f"{'='*60}")
    
    for company in df["company"].unique():
        cdf = df[df["company"] == company]
        
        kwh_base_sum = cdf["kwh"].sum()
        kvarh_base_sum = cdf["kvarh"].sum()
        kwh_base_max = cdf["kwh"].max()
        
        # Cálculos aplicando múltiplo
        kwh_final = kwh_base_sum * multiplo
        kvarh_final = kvarh_base_sum * multiplo
        
        # KW: Potencia = Energía_intervalo / tiempo_intervalo
        # Si los datos son energía por intervalo de 15 min (0.25 h):
        # KW_intervalo = kWh_intervalo / 0.25 = kWh_intervalo * 4
        # Pero el "KW" reportado suele ser la DEMANDA MÁXIMA:
        # kw_demanda_max = max(kWh_intervalo) * 4 * multiplo
        
        # Opción 1: KW = MAX(kWh) * multiplo (asume kWh ya es potencia promedio)
        kw_option1 = kwh_base_max * multiplo
        
        # Opción 2: KW = MAX(kWh) * 4 * multiplo (asume kWh es energía de 15 min)
        kw_option2 = kwh_base_max * 4 * multiplo
        
        # Opción 3: KW = MEAN(kWh != 0) * 4 * multiplo
        non_zero_kwh = cdf[cdf["kwh"] > 0]["kwh"]
        kwh_mean_nz = non_zero_kwh.mean() if len(non_zero_kwh) > 0 else 0
        kw_option3 = kwh_mean_nz * 4 * multiplo
        
        print(f"\n--- {company[:50]} ---")
        print(f"  Filas: {len(cdf)}")
        print(f"  kWh base: sum={kwh_base_sum:.6f}, max={kwh_base_max:.6f}")
        print(f"  kVArh base: sum={kvarh_base_sum:.6f}")
        print(f"")
        print(f"  KWh FINAL  = {kwh_base_sum:.6f} × {multiplo} = {kwh_final:.2f}")
        print(f"  KVARh FINAL = {kvarh_base_sum:.6f} × {multiplo} = {kvarh_final:.2f}")
        print(f"")
        print(f"  KW opciones:")
        print(f"    Opción 1: MAX(kWh) × multiplo           = {kwh_base_max:.6f} × {multiplo} = {kw_option1:.2f}")
        print(f"    Opción 2: MAX(kWh) × 4 × multiplo       = {kwh_base_max:.6f} × 4 × {multiplo} = {kw_option2:.2f}")
        print(f"    Opción 3: MEAN(kWh>0) × 4 × multiplo    = {kwh_mean_nz:.6f} × 4 × {multiplo} = {kw_option3:.2f}")
        
        # Si conoces el valor esperado, descomenta y ajusta:
        # expected_kwh = 12345.67
        # expected_kvarh = 6789.01
        # expected_kw = 123.45
        # print(f"\n  COMPARACIÓN CON ESPERADO:")
        # print(f"    KWh:   calculado={kwh_final:.2f}, esperado={expected_kwh}")
        # print(f"    KVARh: calculado={kvarh_final:.2f}, esperado={expected_kvarh}")
        # print(f"    KW:    opciones arriba vs esperado={expected_kw}")


def main():
    print("="*70)
    print("DIAGNÓSTICO DE DISCREPANCIAS EN DATOS KV2C")
    print("="*70)
    
    # Probar cada carpeta
    for folder in INPUT_FOLDERS:
        if folder.exists():
            run_full_analysis(folder, TEST_MONTH, TEST_YEAR, MULTIPLO)
            break  # Procesar solo la primera carpeta que existe
    else:
        print("\nNinguna de las carpetas configuradas existe.")
        print("Por favor edita INPUT_FOLDERS en este script.")


if __name__ == "__main__":
    main()
