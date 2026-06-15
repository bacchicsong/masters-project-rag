import json
import os
from pathlib import Path
from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams, PointStruct
from sentence_transformers import SentenceTransformer, InputExample
from sentence_transformers import losses
from torch.utils.data import DataLoader

from config.config import RAG_CONFIG

QDRANT_TIMEOUT = 180
EMBEDDING_MODEL_NAME = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
FINE_TUNED_MODEL_PATH = str(Path(__file__).parent.parent.parent / "models" / "fine_tuned_bi_encoder")
USE_FINE_TUNED = False

def chunk_text(text, size=100, overlap=20):
    words = text.split()
    chunks = []
    for i in range(0, len(words), size - overlap):
        chunks.append(" ".join(words[i:i+size]))
    return chunks

def get_train_examples():
    # Import locally to avoid circular import
    from tools.fill_qdrant import load_json_files
    train_examples = []
    texts = load_json_files("data")
    for doc in texts:
        chunks = chunk_text(doc)
        for i in range(len(chunks) - 1):
            train_examples.append(InputExample(texts=[chunks[i], chunks[i+1]]))
    return train_examples


def get_embedded_model() -> SentenceTransformer:
    if USE_FINE_TUNED and os.path.isdir(FINE_TUNED_MODEL_PATH):
        model = SentenceTransformer(FINE_TUNED_MODEL_PATH)
        return model

    # Load the base pre-trained model directly (no fine-tuning)
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    return model


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


def insert_document(qdrant: QdrantClient, collection_name: str, doc: dict, idx: int):
    model = get_embedded_model()

    text_to_embed = doc.get("text", "")
    
    if not text_to_embed:
        return

    vector = model.encode(text_to_embed, normalize_embeddings=True).tolist()

    payload = doc.copy()

    point = PointStruct(id=idx, vector=vector, payload=payload)
    qdrant.upsert(collection_name=collection_name, points=[point])
