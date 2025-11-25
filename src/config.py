from pathlib import Path
from dotenv import load_dotenv
import os

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
REF_DIR = BASE_DIR / "reference"

ENV_PATH = BASE_DIR / "config" / ".env"
load_dotenv(ENV_PATH)

SALT = os.environ["SALT"]
