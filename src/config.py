# config.py
from pathlib import Path
from dotenv import load_dotenv
import os

PROJECT_ROOT = Path(__file__).resolve().parent  # adjust if needed

RAW_DIR = PROJECT_ROOT / "raw"
REF_DIR = PROJECT_ROOT / "reference"

ENV_PATH = PROJECT_ROOT / "config" / ".env"
load_dotenv(ENV_PATH)

SALT = os.environ["SALT"]
