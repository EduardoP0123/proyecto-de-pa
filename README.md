# proyecto de pa

Estructura y API mínima:
- src/csv_processor.py: motor de procesamiento (detección de fecha, filtrado, rellenado, export)
- src/ui_components.py: puntos de integración con la UI
- config/: settings y logging
- Para integrar, la UI actual debe llamar a `src.ui_components.run_ui(Path(workspace))`
