# src/config.py
from pathlib import Path
from dotenv import load_dotenv
import os

# Project root directory (one level above src/)
BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "raw"
REF_DIR = BASE_DIR / "reference"

ENV_PATH = BASE_DIR / "config" / ".env"
load_dotenv(ENV_PATH)

SALT = os.environ["SALT"]
