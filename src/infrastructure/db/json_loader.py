import json
from pathlib import Path


def load_json_files(directory: str) -> list[dict]:
    json_dir = Path(directory)
    all_docs = []

    for file in json_dir.glob("*.json"):
        with file.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                all_docs.extend(data)
            elif isinstance(data, dict):
                all_docs.append(data)
            else:
                raise ValueError(f"Неподдерживаемый формат JSON в файле {file.name}")

    return all_docs