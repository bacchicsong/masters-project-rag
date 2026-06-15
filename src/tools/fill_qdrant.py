import time
from pathlib import Path

from infrastructure.db.qdrand import init_qdrant, insert_document, get_embedded_model
from infrastructure.db.json_loader import load_json_files
from infrastructure.di.dependencies import get_logger
from config.config import RAG_CONFIG


def run():
    logger = get_logger()
    qdrant = init_qdrant(logger)
    docs = load_json_files("data")
    total = len(docs)

    if total == 0:
        logger.warning("No documents found in 'data' directory. Nothing to load.")
        return

    logger.info(f"Found {total} document(s) to load into Qdrant collection '{RAG_CONFIG.QDRANT_COLLECTION_NAME}'")

    # Load model ONCE before the loop to avoid reloading weights per document
    t_model_start = time.time()
    logger.info("Loading embedding model (one-time)...")
    model = get_embedded_model()
    model_time = time.time() - t_model_start
    logger.info(f"Embedding model loaded in {model_time:.1f}s")

    t_start = time.time()
    skipped = 0

    for idx, doc in enumerate(docs, start=1):
        insert_document(qdrant, RAG_CONFIG.QDRANT_COLLECTION_NAME, doc, idx, model=model)
        title = doc.get("title", doc.get("source", f"doc#{idx}"))
        elapsed = time.time() - t_start
        speed = idx / elapsed if elapsed > 0 else 0
        logger.info(
            f"[{idx}/{total}] Loaded: {title}  "
            f"({elapsed:.1f}s elapsed, {speed:.1f} docs/s)"
        )

    total_time = time.time() - t_start
    logger.info(
        f"Done! {total - skipped}/{total} documents loaded into Qdrant "
        f"in {total_time:.1f}s (model load: {model_time:.1f}s)"
    )


if __name__ == "__main__":
    run()