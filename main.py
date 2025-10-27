import sys
from pathlib import Path

# Agregar raíz al path
project_root = Path(__file__).resolve().parent
sys.path.insert(0, str(project_root))

from config.logging_config import configure_logging
from config.settings import DEFAULT_WORKSPACE_ROOT
from src.ui_components import run_ui

def main():
    configure_logging()
    workspace = Path(DEFAULT_WORKSPACE_ROOT)
    run_ui(workspace)  # la UI actual debe llamar al backend por medio de esta función

if __name__ == "__main__":
    main()
