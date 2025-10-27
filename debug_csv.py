import pandas as pd
from pathlib import Path
import sys

try:
    folder = Path(r"C:\Users\User\Downloads\BILLREAD_WORKSPACE_ui")
    
    if not folder.exists():
        print(f"❌ La carpeta no existe: {folder}")
        sys.exit(1)
    
    csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        print(f"❌ No se encontraron archivos CSV en: {folder}")
        sys.exit(1)
    
    print(f"✓ Se encontraron {len(csv_files)} archivos CSV")
    print(f"✓ Analizando primer archivo...\n")
    
    csv_file = csv_files[0]
    
    print(f"{'='*70}")
    print(f"Archivo: {csv_file.name}")
    print(f"{'='*70}\n")
    
    # Leer primeras líneas RAW
    print("📄 PRIMERAS 15 LÍNEAS DEL ARCHIVO (RAW):")
    print("-" * 70)
    with open(csv_file, 'r', encoding='utf-8-sig', errors='ignore') as f:
        for i, line in enumerate(f.readlines()[:15], 1):
            print(f"{i:2d}: {line.rstrip()}")
    
    print("\n" + "="*70)
    print("\n📊 CARGANDO CON PANDAS:")
    print("-" * 70)
    
    # Intentar cargar con pandas
    df = pd.read_csv(csv_file, encoding='utf-8-sig')
    
    print(f"✓ Total filas: {len(df)}")
    print(f"✓ Columnas: {list(df.columns)}")
    print(f"\n📋 PRIMERAS 5 FILAS:")
    print(df.head(5))
    
    print(f"\n🔍 INFO DE COLUMNAS:")
    print(df.info())
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    import traceback
    traceback.print_exc()