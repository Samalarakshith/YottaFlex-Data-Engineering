"""Launcher for the YottaFlex Web Interface."""

import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.web.app import run_server


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5050))
    run_server(port)
