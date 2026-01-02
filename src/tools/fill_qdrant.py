import json
from pathlib import Path

from infrastructure.db.qdrand import init_qdrant, insert_document
from config.config import config

def load_json_files(directory: str) -> list[dict[str, str]]:
    json_dir = Path(directory)
    all_docs = []
    
    for file in json_dir.glob("*.json"):
        with file.open("r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                if "sections" in data:
                    all_docs.extend(data["sections"])
            elif isinstance(data, list):
                for item in data:
                    if isinstance(item, dict) and "sections" in item:
                        all_docs.extend(item["sections"])
            else:
                raise ValueError(f"Unsupported file format in {file.name}")
                
    return all_docs

def run():
    qdrant = init_qdrant()
    docs = load_json_files("data")
    for idx, doc in enumerate(docs, start=1):
        insert_document(qdrant, config.QDRANT_COLLECTION_NAME, doc, idx)
    print("Docs have been sucessfully loaded")

if __name__ == "__main__":
    run()
