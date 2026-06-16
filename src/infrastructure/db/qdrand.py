import json
import logging
import os
import threading
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer, InputExample
from sentence_transformers import losses
from torch.utils.data import DataLoader

from config.config import RAG_CONFIG

QDRANT_TIMEOUT = 180
EMBEDDING_MODEL_NAME = RAG_CONFIG.EMBEDDING_MODEL_NAME
FINE_TUNED_MODEL_PATH = str(Path(__file__).parent.parent.parent / "models" / "fine_tuned_bi_encoder")
USE_FINE_TUNED = os.getenv("USE_FINE_TUNED_MODEL", "false").lower() == "true"
MODEL_CACHE_FOLDER = os.getenv("SENTENCE_TRANSFORMERS_HOME") or None

# ── Cached embedding model (singleton) ──────────────────────────────
_embedded_model: "SentenceTransformer | None" = None
_embedded_model_lock = threading.Lock()
_embedded_model_status = {
    "state": "not_loaded",
    "model_name": EMBEDDING_MODEL_NAME,
    "dimension": None,
    "error": None,
}
logger = logging.getLogger("app_logger")

def chunk_text(text, size=100, overlap=20):
    words = text.split()
    chunks = []
    for i in range(0, len(words), size - overlap):
        chunks.append(" ".join(words[i:i+size]))
    return chunks

def get_train_examples():
    from infrastructure.db.json_loader import load_json_files
    train_examples = []
    texts = load_json_files("data")
    for doc in texts:
        chunks = chunk_text(doc)
        for i in range(len(chunks) - 1):
            train_examples.append(InputExample(texts=[chunks[i], chunks[i+1]]))
    return train_examples


def get_embedded_model() -> SentenceTransformer:
    global _embedded_model
    if _embedded_model is not None:
        return _embedded_model

    with _embedded_model_lock:
        if _embedded_model is not None:
            return _embedded_model

        model_name = (
            FINE_TUNED_MODEL_PATH
            if USE_FINE_TUNED and os.path.isdir(FINE_TUNED_MODEL_PATH)
            else EMBEDDING_MODEL_NAME
        )
        _embedded_model_status.update(
            {
                "state": "loading",
                "model_name": model_name,
                "dimension": None,
                "error": None,
            }
        )
        logger.info(f"Loading embedding model: {model_name}")
        try:
            _embedded_model = SentenceTransformer(
                model_name,
                cache_folder=MODEL_CACHE_FOLDER,
            )
            _embedded_model_status.update(
                {
                    "state": "loaded",
                    "dimension": _embedded_model.get_sentence_embedding_dimension(),
                    "error": None,
                }
            )
            logger.info(
                "Embedding model loaded",
                extra={
                    "model_name": model_name,
                    "dimension": _embedded_model_status["dimension"],
                },
            )
        except Exception as exc:
            _embedded_model_status.update(
                {
                    "state": "failed",
                    "error": str(exc),
                }
            )
            logger.exception("Embedding model loading failed")
            raise
    return _embedded_model


def get_embedding_model_status() -> dict:
    return dict(_embedded_model_status)


def init_qdrant(logger) -> QdrantClient:
    global collection_exists
    qdrant = QdrantClient(
        url=f"http://{RAG_CONFIG.QDRANT_HOST}:{RAG_CONFIG.QDRANT_PORT}",
        timeout=QDRANT_TIMEOUT,
    )
    collection_name = RAG_CONFIG.QDRANT_COLLECTION_NAME
    collection_exists = qdrant.collection_exists(collection_name=collection_name)
    if not collection_exists:
        qdrant.create_collection(
            collection_name=collection_name,
            vectors_config=VectorParams(size=384, distance=Distance.COSINE),
        )
        collection_exists = True
        logger.info(f"Новая коллекция '{collection_name}' была создана")
    return qdrant


def insert_document(
    qdrant: QdrantClient,
    collection_name: str,
    doc: dict,
    idx: int,
    *,
    model: SentenceTransformer | None = None,
):
    if model is None:
        model = get_embedded_model()

    text_to_embed = doc.get("text", "")

    if not text_to_embed:
        return

    vector = model.encode(text_to_embed, normalize_embeddings=True).tolist()

    payload = doc.copy()

    point = PointStruct(id=idx, vector=vector, payload=payload)
    qdrant.upsert(collection_name=collection_name, points=[point])
