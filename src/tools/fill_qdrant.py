import json
from pathlib import Path

from infrastructure.db.qdrand import init_qdrant, insert_document
from config.config import config

def load_json_files(directory: str) -> list[dict[str, str]]:
    json_dir = Path(directory)
    return [json.loads(f.read_text(encoding="utf-8")) for f in json_dir.glob("*.json")]

def run():
    qdrant = init_qdrant()
    docs = load_json_files("data")
    for idx, doc in enumerate(docs, start=1):
        insert_document(qdrant, config.QDRANT_COLLECTION_NAME, doc, idx)
    print("Docs have been sucessfully loaded")

if __name__ == "__main__":
    run()
