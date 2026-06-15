from pathlib import Path

from infrastructure.db.qdrand import init_qdrant, insert_document
from infrastructure.db.json_loader import load_json_files
from infrastructure.di.dependencies import get_logger
from config.config import RAG_CONFIG


def run():
    logger = get_logger()
    qdrant = init_qdrant(logger)
    docs = load_json_files("data")
    for idx, doc in enumerate(docs, start=1):
        insert_document(qdrant, RAG_CONFIG.QDRANT_COLLECTION_NAME, doc, idx)
    print("Docs have been sucessfully loaded")


if __name__ == "__main__":
    run()