import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "api.yaml"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def