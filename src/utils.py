import time
import json
from pathlib import Path

RAW_DIR = Path(__file__).resolve().parent.parent / "data_raw"
RAW_DIR.mkdir(exist_ok = True)


def save_raw(obj, name: str):
    path = RAW_DIR / f'{name}.json'
    with open(path, 'w') as f:
        json.dump(obj, f, indent=2)
    return str(path)

def rate_limit_sleep(seconds=12):
    time.sleep(seconds)