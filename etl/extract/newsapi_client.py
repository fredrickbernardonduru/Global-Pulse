import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "configs" / "api.yaml"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as file:
        return yaml.safe_load(file)


def fetch_top_headlines():
    load_dotenv()

    api_key = os.getenv("NEWS_API_KEY")
    if not api_key:
        raise ValueError("NEWS_API_KEY is missing. Add it to your .env file.")

    config = load_config()["newsapi"]

    url = f"{config['base_url']}/{config['endpoint']}"

    params = {
        "country": config["country"],
        "category": config["category"],
        "pageSize": config["page_size"],
    }

    headers = {
        "X-Api-Key": api_key
    }

    response = requests.get(url, params=params, headers=headers, timeout=20)

    if response.status_code != 200:
        raise Exception(f"NewsAPI request failed: {response.status_code} - {response.text}")

    return response.json()


def save_raw_response(data):
    RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = RAW_DATA_DIR / f"newsapi_top_headlines_{timestamp}.json"

    with open(file_path, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=2, ensure_ascii=False)

    return file_path


def validate_response(data):
    if data.get("status") != "ok":
        raise ValueError(f"API returned bad status: {data}")

    if "articles" not in data:
        raise ValueError("Missing 'articles' field in response.")

    if not isinstance(data["articles"], list):
        raise TypeError("'articles' must be a list.")

    print(f"Validation passed.")
    print(f"Total results: {data.get('totalResults')}")
    print(f"Articles received: {len(data['articles'])}")


if __name__ == "__main__":
    print("Fetching top headlines from NewsAPI...")

    raw_data = fetch_top_headlines()
    validate_response(raw_data)

    saved_path = save_raw_response(raw_data)

    print(f"Raw data saved to: {saved_path}")